import logging
import os
import io
import json
import config
import pandas as pd
from google.cloud import storage, pubsub
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)

batch_settings = pubsub_v1.types.BatchSettings(**config.TOPIC_BATCH_SETTINGS)
publisher = pubsub.PublisherClient(batch_settings)


def gather_publish_msg(msg):
    if hasattr(config, 'COLUMNS_PUBLISH'):
        gathered_msg = {}
        for msg_key, value_key in config.COLUMNS_PUBLISH.items():
            if type(value_key) == dict and 'source_attribute' in value_key:
                gathered_msg[msg_key] = msg[value_key['source_attribute']]

                if 'conversion' in value_key:
                    if value_key['conversion'] == 'lowercase':
                        gathered_msg[msg_key] = gathered_msg[msg_key].lower()
                    elif value_key['conversion'] == 'uppercase':
                        gathered_msg[msg_key] = gathered_msg[msg_key].upper()
                    elif value_key['conversion'] == 'capitalize':
                        gathered_msg[msg_key] = \
                            gathered_msg[msg_key].capitalize()
            else:
                gathered_msg[msg_key] = msg[value_key]
        return gathered_msg
    return msg


def publish_json(msg, rowcount, rowmax, topic_project_id, topic_name):
    topic_path = publisher.topic_path(topic_project_id, topic_name)
    # logging.info(f'Publish to {topic_path}: {msg}')
    future = publisher.publish(
        topic_path, bytes(json.dumps(msg).encode('utf-8')))
    future.add_done_callback(
        lambda x: logging.info(
            'Published msg with ID {} ({}/{} rows).'.format(
                future.result(), rowcount, rowmax))
    )


def calculate_diff(df_old, df_new):
    if len(df_old.columns) != len(df_new.columns):
        logging.info('Different columns found')
        if hasattr(config, 'COLUMNS_NONPII') and len(set(df_new.columns) - set(config.COLUMNS_NONPII)) > 0:
            logging.warning('Not correct columns found in new file')
            raise ValueError('Not correct columns found in new file')
        return df_new

    columns_drop = getattr(config, 'COLUMNS_DROP', [])
    joined = df_old.\
        drop_duplicates().\
        drop(columns_drop, axis=1).\
        merge(
            df_new.
            drop_duplicates().
            drop(columns_drop, axis=1),
            how='right',
            indicator=True)
    diff = joined.query("_merge != 'both'").drop('_merge', axis=1)
    return diff


def df_from_store(bucket_name, blob_name, from_archive=False):
    path = 'gs://{}/{}'.format(bucket_name, blob_name)
    logging.info('Reading {}'.format(path))
    if blob_name.endswith('.xlsx'):
        if from_archive:
            df = pd.read_excel(path, dtype=str)
        else:
            converter = {i: str for i in range(len(config.COLUMNS_NONPII))}
            df = pd.read_excel(path, converters=converter)
    if blob_name.endswith('.csv'):
        df = pd.read_csv(path, **config.CSV_DIALECT_PARAMETERS)
    if blob_name.endswith('.json'):
        if hasattr(config, 'ATTRIBUTE_WITH_THE_LIST'):
            bucket = storage.Client().get_bucket(bucket_name)
            json_data = json.loads(bucket.get_blob(blob_name).download_as_string())
            df = pd.DataFrame(json_data[config.ATTRIBUTE_WITH_THE_LIST])
        else:
            df = pd.read_json(path, dtype=False)
    logging.info('Read file {} from {}'.format(blob_name, bucket_name))
    return df


def df_to_store(bucket_name, blob_name, df):
    # Different types of files: xlsx or json
    if blob_name.endswith('.xlsx'):
        new_blob = blob_name
        strIO = io.BytesIO()
        excel_writer = pd.ExcelWriter(strIO, engine="xlsxwriter")
        df.to_excel(excel_writer, sheet_name="data", index=False)
        excel_writer.save()
        file = strIO.getvalue()
        content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif blob_name.endswith('.csv'):
        new_blob = blob_name
        file = df.to_csv(**config.CSV_DIALECT_PARAMETERS)
        content_type = 'text/csv'
    else:
        new_blob = os.path.splitext(blob_name)[0] + '.json'
        blob_str = df.to_json()
        blob_json = json.loads(blob_str)
        if hasattr(config, 'ATTRIBUTE_WITH_THE_LIST'):
            blob_data = {config.ATTRIBUTE_WITH_THE_LIST: blob_json}
        else:
            blob_data = blob_json
        file = json.dumps(blob_data).encode('utf-8')
        content_type = 'application/json'

    # Create blob
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = storage.Blob(new_blob, bucket)

    # Upload blob
    blob.upload_from_string(
        file,
        content_type=content_type
    )
    logging.info('Write file {} to {}'.format(new_blob, bucket_name))


def remove_from_store(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    logging.info('Deleted file {} from {}'.format(blob_name, bucket_name))


def get_prev_blob(bucket_name, prefix_filter):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix_filter))
    if blobs:

        def compare_lobs(lob):
            return lob.updated

        blobs.sort(key=compare_lobs, reverse=True)

        if config.ARCHIVE == config.INBOX:
            if len(blobs) > 1:
                return blobs[1].name
            else:
                return None
        else:
            return blobs[0].name
    else:
        return None


def publish_diff(data, context):
    logging.info('Run started')

    bucket = data['bucket']
    filename = data['name']
    df_new = None

    prefix_filter = config.FILEPATH_PREFIX_FILTER if hasattr(config, 'FILEPATH_PREFIX_FILTER') else None

    if not prefix_filter or filename.startswith(prefix_filter):
        try:
            # Read dataframe from store
            df_new = df_from_store(bucket, filename)
            # Get previous data from archive
            blob_prev = get_prev_blob(config.ARCHIVE, prefix_filter)
            full_load = config.FULL_LOAD if hasattr(config, 'FULL_LOAD') else False

            # Read previous data from archive and compare
            if blob_prev and (not full_load):
                df_prev = df_from_store(config.ARCHIVE, blob_prev, from_archive=True)
                df_diff = calculate_diff(df_prev, df_new)
            else:
                df_diff = df_new.copy().drop_duplicates()

            # Check the number of new records
            # In case of no new records: don't send any updates
            if len(df_diff) == 0:
                logging.info('No new rows found')
            else:
                logging.info('Found {} new rows.'.format(len(df_diff)))

                # Export to json
                rows_str = df_diff.to_json(orient='records')
                rows_json = json.loads(rows_str)

                # Publish individual rows to topic
                i = 1
                for row in rows_json:
                    publish_json(gather_publish_msg(row), rowcount=i, rowmax=len(rows_json), **config.TOPIC_SETTINGS)
                    i += 1

            if config.INBOX != config.ARCHIVE:
                # Write file to archive
                df_to_store(config.ARCHIVE, filename, df_new)
                # Remove file from inbox
                remove_from_store(config.INBOX, filename)

            logging.info('Run succeeded')

        except Exception as e:
            if hasattr(config, 'ERROR'):
                df_to_store(config.ERROR, filename, df_new)
            if config.INBOX != config.ARCHIVE:
                remove_from_store(config.INBOX, filename)
            logging.error('Processing failure {}'.format(e))
            raise
        finally:
            logging.info('Run done')
    else:
        logging.info(f'Skipping {filename} due to FILEPATH_PREFIX_FILTER {prefix_filter}')


# main defined for testing
if __name__ == '__main__':
    publish_diff({'bucket': config.INBOX, 'name': 'testfile.json'}, {'event_id': 0, 'event_type': 'none'})
