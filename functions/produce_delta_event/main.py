import os
import io
import json
import config
import logging
import brotli
import pandas as pd

from gobits import Gobits
from google.cloud import storage, pubsub_v1

from gathermsg import gather_publish_msg


logging.basicConfig(level=logging.INFO)

batch_settings = pubsub_v1.types.BatchSettings(**config.TOPIC_BATCH_SETTINGS)
publisher = pubsub_v1.PublisherClient(batch_settings)


def publish_json(gobits, innermsg, rowcount, rowmax, topic_project_id, topic_name, subject=None):
    topic_path = publisher.topic_path(topic_project_id, topic_name)

    if subject:
        sendmsg = {
            "gobits": [gobits.to_json()],
            subject: innermsg
        }
    else:
        sendmsg = innermsg

    # logging.info(f'Publish to {topic_path}: {sendmsg}')
    future = publisher.publish(
        topic_path, bytes(json.dumps(sendmsg).encode('utf-8')))
    future.add_done_callback(
        lambda x: logging.info(
            'Published msg with ID {} ({}/{} rows).'.format(
                future.result(), rowcount, rowmax))
    )


def calculate_diff(df_old, df_new):
    if len(df_old.columns) != len(df_new.columns) or df_old.empty:
        if df_old.empty:
            logging.info('Previous state is empty. Rebuilding.')
        else:
            logging.info('Different columns found')
        if hasattr(config, 'COLUMNS_NONPII') and len(set(df_new.columns) - set(config.COLUMNS_NONPII)) > 0:
            logging.warning('Not correct columns found in new file')
            raise ValueError('Not correct columns found in new file')
        return df_new

    columns_drop = getattr(config, 'COLUMNS_DROP', [])
    joined = df_old.\
        drop(columns_drop, axis=1).\
        merge(
            df_new.
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
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.get_blob(blob_name)
            if blob.content_encoding == 'br':
                content = blob.download_as_string(raw_download=True)
                content = brotli.decompress(content)
            else:
                content = blob.download_as_string()
            json_data = json.loads(content)
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

    prefix_filter = config.FILEPATH_PREFIX_FILTER if hasattr(config, 'FILEPATH_PREFIX_FILTER') else None
    columns_publish = config.COLUMNS_PUBLISH if hasattr(config, 'COLUMNS_PUBLISH') else None
    batch_message_size = config.BATCH_MESSAGE_SIZE if hasattr(config, 'BATCH_MESSAGE_SIZE') else None

    if not prefix_filter or filename.startswith(prefix_filter):

        df_orig = None

        try:
            # Read dataframe from store
            df_orig = df_from_store(bucket, filename)
            full_load = config.FULL_LOAD if hasattr(config, 'FULL_LOAD') else False
            should_drop_duplicates = config.SHOULD_DROP_DUPLICATES \
                if hasattr(config, 'SHOULD_DROP_DUPLICATES') else True

            if should_drop_duplicates:
                df_new = df_orig.copy().drop_duplicates()
            else:
                df_new = df_orig

            # Read previous data from archive and compare
            if not full_load:
                # Get previous data from archive
                blob_prev = get_prev_blob(config.ARCHIVE, prefix_filter)
                if blob_prev:
                    df_prev = df_from_store(config.ARCHIVE, blob_prev, from_archive=True)
                    if should_drop_duplicates:
                        df_prev.drop_duplicates()
                    df_diff = calculate_diff(df_prev, df_new)
                else:
                    full_load = True

            if full_load:
                df_diff = df_new

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
                message_batch = []
                gobits = Gobits.from_context(context=context)

                for row in rows_json:
                    msg_to_publish = gather_publish_msg(row, columns_publish)
                    if not batch_message_size:
                        publish_json(gobits, msg_to_publish, rowcount=i, rowmax=len(rows_json),
                                     **config.TOPIC_SETTINGS)
                    else:
                        message_batch.append(msg_to_publish)
                        if len(message_batch) == batch_message_size:
                            publish_json(gobits, message_batch, rowcount=i, rowmax=len(rows_json),
                                         **config.TOPIC_SETTINGS)
                            message_batch = []

                    i += 1

                if message_batch:
                    publish_json(gobits, message_batch, rowcount=i, rowmax=len(rows_json),
                                 **config.TOPIC_SETTINGS)

            if config.INBOX != config.ARCHIVE:
                # Write file to archive
                df_to_store(config.ARCHIVE, filename, df_orig)
                # Remove file from inbox
                remove_from_store(config.INBOX, filename)

            logging.info('Run succeeded')

        except Exception as e:
            if hasattr(config, 'ERROR'):
                df_to_store(config.ERROR, filename, df_orig)
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
