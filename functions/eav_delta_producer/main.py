import utils
import json
import config
import logging
import pandas as pd

from google.cloud import pubsub_v1
from datetime import datetime, timedelta
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import create_engine, or_
from models import ImportMeasureValues, ImportKeys, Subscriptions

db_password = utils.get_secret(
    config.database['project_id'],
    config.database['secret_name']
)

sacn = 'mysql+pymysql://{}:{}@/{}?unix_socket=/cloudsql/{}:{}:{}'.format(
    config.database['db_user'],
    db_password,
    config.database['db_name'],
    config.database['project_id'],
    config.database['region'],
    config.database['instance_id']
)


def handler(request):
    source = config.SOURCE
    logging.info(f'Starting query for source {source}')
    df = query(source)

    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    df_yesterday = query(source, yesterday)

    if not config.FULL_LOAD:
        df = difference(df_yesterday, df)

    rows_str = df.to_json(orient='records')
    rows_json = json.loads(rows_str)

    i = 1
    for row in rows_json:
        publish_json(row, rowcount=i, rowmax=len(rows_json), **config.TOPIC_SETTINGS)
        i += 1


def publish_json(msg, rowcount, rowmax, topic_project_id, topic_name):
    batch_settings = pubsub_v1.types.BatchSettings(**config.TOPIC_BATCH_SETTINGS)
    publisher = pubsub_v1.PublisherClient(batch_settings)
    topic_path = publisher.topic_path(topic_project_id, topic_name)
    future = publisher.publish(
        topic_path, bytes(json.dumps(msg).encode('utf-8')))
    future.add_done_callback(
        lambda x: logging.info(
            'Published msg with ID {} ({}/{} rows).'.format(
                future.result(), rowcount, rowmax))
    )


def difference(df_old, df_new):
    joined = df_old.drop_duplicates().merge(df_new.drop_duplicates(), how='right', indicator=True)
    diff = joined.query("_merge != 'both'").drop('_merge', axis=1)
    logging.info(f'Difference is {len(diff)} records!')
    return diff


def query(sourceTag, ts=None):
    engine = create_engine(sacn)
    session = sessionmaker(engine, autoflush=False, autocommit=False)()

    if ts == "all":
        q = session.query([
            ImportMeasureValues.sourceKey,
            ImportMeasureValues.measure,
            ImportMeasureValues.value,
            ImportKeys.version,
            ImportKeys.versionEnd]
        )
        set_index = [0, 3, 4, 1]
    else:
        q = session.query(
            ImportMeasureValues.sourceKey,
            ImportMeasureValues.measure,
            ImportMeasureValues.value
        )
        set_index = [0, 1]

    q = q.join(ImportKeys, ImportMeasureValues.importId == ImportKeys.importId).\
        join(Subscriptions, ImportKeys.sourceTag == Subscriptions.stagingSourceTag).\
        filter(ImportKeys.delete == 0)

    if ts is None:
        q = q.filter(ImportKeys.versionEnd.is_(None))
    elif ts != 'all':
        q = q.filter(ImportKeys.version <= ts).\
            filter(or_(ImportKeys.versionEnd.is_(None), ImportKeys.versionEnd > ts))

    q = q.filter(Subscriptions.sourceTag == sourceTag)

    df = session.execute(q)
    df = pd.DataFrame(df)
    df = df.replace('\n', ' ', regex=True)
    df = df.replace(' +', ' ', regex=True)

    logging.info(f'Found {len(df)} records!')

    if len(df) == 0:
        df = pd.DataFrame([])
    else:
        df = df.set_index(set_index).unstack()
        df.columns = df.columns.levels[1].tolist()
        df.reset_index(inplace=True)
        df = df.rename(columns={
            0: 'sourceKey',
            3: 'version',
            4: 'versionEnd',
        })
        df.columns.name = ''
        df.set_index('sourceKey', inplace=True)

        # SourceTag specific additions:
        # to_add = config.COLS.get(sourceTag, None)
        # if to_add is not None:
        #     for col in to_add:
        #         if col not in list(df):
        #             df[col] = ''

    return df
