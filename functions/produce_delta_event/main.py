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


def publish_json(msg, rowcount, rowmax, topic_project_id, topic_name):
    topic_path = publisher.topic_path(topic_project_id, topic_name)
    future = publisher.publish(
        topic_path, bytes(json.dumps(msg).encode('utf-8')))
    future.add_done_callback(
        lambda x: logging.info(
            'Published msg with ID {} ({}/{} rows).'.format(
                future.result(), rowcount, rowmax))
    )


def calculate_diff(df_old, df_new):
    columns_drop = config.COLUMNS_DROP if hasattr(config, 'COLUMNS_DROP') else []
    joined = df_old.\
        drop_duplicates().\
        drop(columns_drop, axis = 1).\
        merge(
            df_new.\
                drop_duplicates().\
                drop(columns_drop, axis = 1),
            how='right', 
            indicator=True)
    diff = joined.query("_merge != 'both'").drop('_merge', axis=1)
    return diff


def df_from_store(bucket_name, blob_name):
    path = 'gs://{}/{}'.format(bucket_name, blob_name)
    logging.info('Reading {}'.format(path))
    if blob_name.endswith('.xlsx'):
        converter = {i: str for i in range(len(config.COLUMNS_NONPII))}
        df = pd.read_excel(path, converters=converter)
    if blob_name.endswith('.csv'):
        df = pd.read_csv(path, **config.CSV_DIALECT_PARAMETERS)
    if blob_name.endswith('.json'):
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
        file = json.dumps(blob_json).encode('utf-8')
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


def get_prev_blob(bucket_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs())
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

    try:
        # Read dataframe from store
        df_new = df_from_store(bucket, filename)
        # Get previous data from archive
        blob_prev = get_prev_blob(config.ARCHIVE)
        full_load = config.FULL_LOAD if hasattr(config, 'FULL_LOAD') else False

        # Read previous data from archive and compare
        if blob_prev and (not full_load):
            df_prev = df_from_store(config.ARCHIVE, blob_prev)
            cols_prev = set(df_prev.columns)
            cols_new = set(df_new.columns)
            # When there are different columns in new file with respect to the old one,
            # the file is rejected.
            if cols_prev != cols_new:
                diff = cols_new - cols_prev
                raise ValueError("Different schema: {} '{}' found in new delivery but not in previous delivery".format(
                    'column' if len(diff) == 1 else 'columns',
                    "', '".join(list(diff)),
                ))
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
                publish_json(row, rowcount=i, rowmax=len(rows_json), **config.TOPIC_SETTINGS)
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


# main defined for testing
if __name__ == '__main__':
    publish_diff({'bucket': 'my-inbox-bucket', 'name': 'testfile.csv'}, {'event_id': 0, 'event_type': 'none'})
