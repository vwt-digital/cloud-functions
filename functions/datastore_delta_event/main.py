import logging
import os
import io
import json
import config
import pandas as pd
from google.cloud import storage, pubsub
from google.cloud import datastore
from google.cloud import pubsub_v1

DATASTORE_CHUNK_SIZE = 300

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


def calculate_diff_from_datastore(df_new, state_storage_specification):
    ds_client = datastore.Client()
    rows_result = []
    for df_chunk in [df_new[i:i+DATASTORE_CHUNK_SIZE] for i in range(0, len(df_new), DATASTORE_CHUNK_SIZE)]:
        new_state_items = {}
        for item in df_chunk.to_dict(orient='records'):
            item_to_publish = gather_publish_msg(item)
            new_state_items[item_to_publish[state_storage_specification['id_property']]] = item_to_publish
        keys = [ds_client.key(state_storage_specification['entity_name'], key) for key in new_state_items.keys()]
        # logging.debug(f'keys to query {keys}')
        missing_items = []
        current_state_chunk = ds_client.get_multi(keys, missing=missing_items)
        for current_item in current_state_chunk:
            new_item = new_state_items[current_item.key.id_or_name]
            is_equal = True
            for key_name, key_value in new_item.items():
                if key_name not in current_item:
                    is_equal = False
                    break
                elif key_value != current_item[key_name]:
                    is_equal = False
                    break
            # logging.debug(f'Comparing {current_item} to {new_item} compares {is_equal}')
            if not is_equal:
                rows_result.append(new_item)
        rows_result.extend([new_state_items[missing_item.key.id_or_name] for missing_item in missing_items])
    return rows_result


def store_to_datastore(datastore_state_to_store, state_storage_specification):
    ds_client = datastore.Client()
    entities_to_put = []

    for key, item in datastore_state_to_store.items():
        entity = datastore.Entity(key=ds_client.key(state_storage_specification['entity_name'], key))
        entity.update(item)
        entities_to_put.append(entity)

    ds_client.put_multi(entities_to_put)


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
            state_storage_specification = config.STATE_STORAGE_SPECIFICATION

            if state_storage_specification['type'] == 'datastore':
                rows_json = calculate_diff_from_datastore(df_new, state_storage_specification)
            else:
                raise ValueError(f"Unknown state_storage type {state_storage_specification['type']}")

            # Check the number of new records
            # In case of no new records: don't send any updates
            if len(rows_json) == 0:
                logging.info('No new rows found')
            else:
                logging.info('Found {} new rows.'.format(len(rows_json)))

                # Publish individual rows to topic
                i = 1
                datastore_new_state = {}
                for publish_message in rows_json:
                    publish_json(publish_message, rowcount=i, rowmax=len(rows_json), **config.TOPIC_SETTINGS)
                    i += 1

                    if state_storage_specification['type'] == 'datastore':
                        datastore_new_state[publish_message[state_storage_specification['id_property']]] = publish_message
                        if len(datastore_new_state) > DATASTORE_CHUNK_SIZE:
                            store_to_datastore(datastore_new_state, state_storage_specification)
                            datastore_new_state = {}

                if state_storage_specification['type'] == 'datastore' and datastore_new_state:
                    store_to_datastore(datastore_new_state, state_storage_specification)

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
