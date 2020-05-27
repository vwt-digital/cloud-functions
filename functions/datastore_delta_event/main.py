import logging
import os
import io
import json
import config
import pandas as pd

from google.cloud import storage, pubsub
from google.cloud import datastore
from google.cloud import pubsub_v1

from defusedxml import ElementTree

from gathermsg import gather_publish_msg

DATASTORE_CHUNK_SIZE = 300

logging.basicConfig(level=logging.INFO)

batch_settings = pubsub_v1.types.BatchSettings(**config.TOPIC_BATCH_SETTINGS)
publisher = pubsub.PublisherClient(batch_settings)


def publish_json(msg_data, rowcount, rowmax, topic_project_id, topic_name, subject=None):
    topic_path = publisher.topic_path(topic_project_id, topic_name)
    if subject:
        msg = {
            "gobits": [
                {}
            ],
            subject: msg_data
        }
    else:
        msg = msg_data
    # logging.info(f'Publish to {topic_path}: {msg}')
    future = publisher.publish(
        topic_path, bytes(json.dumps(msg).encode('utf-8')))
    future.add_done_callback(
        lambda x: logging.debug(
            'Published msg with ID {} ({}/{} rows).'.format(
                future.result(), rowcount, rowmax))
    )


def load_odata(xml_data):
    xml_tree = ElementTree.XML(xml_data)
    if not xml_tree.tag.endswith('}feed'):
        logging.warning('Root XML element is expected to be "feed"')
        return pd.DataFrame()
    namespace = xml_tree.tag[:-4]
    entries_list = []
    for entry in xml_tree.findall(f'{namespace}entry'):
        content = entry.find(f'{namespace}content')
        if content:
            entry_dict = {}
            for properties in content:
                if properties.tag.endswith('}properties'):
                    for prop in properties:
                        if prop.text:
                            entry_dict[prop.tag.split('}')[-1]] = prop.text
                    entries_list.append(entry_dict)
    return entries_list


def data_from_store(bucket_name, blob_name, from_archive=False):
    path = 'gs://{}/{}'.format(bucket_name, blob_name)
    logging.info('Reading {}'.format(path))
    if blob_name.endswith('.xlsx'):
        if from_archive:
            new_data = pd.read_excel(path, dtype=str).to_dict(orient='records')
        else:
            new_data = pd.read_excel(path, dtype=str).to_dict(orient='records')
    elif blob_name.endswith('.csv'):
        new_data = pd.read_csv(path, **config.CSV_DIALECT_PARAMETERS).to_dict(orient='records')
    elif blob_name.endswith('.atom'):
        bucket = storage.Client().get_bucket(bucket_name)
        return load_odata(bucket.get_blob(blob_name).download_as_string())
    elif blob_name.endswith('.json'):
        if hasattr(config, 'ATTRIBUTE_WITH_THE_LIST'):
            bucket = storage.Client().get_bucket(bucket_name)
            json_data = json.loads(bucket.get_blob(blob_name).download_as_string())
            new_data = json_data[config.ATTRIBUTE_WITH_THE_LIST]
        else:
            new_data = pd.read_json(path, dtype=False).to_dict(orient='records')
    logging.info('Read file {} from {}'.format(blob_name, bucket_name))
    return new_data


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


def calculate_diff_from_datastore(new_data, state_storage_specification):
    ds_client = datastore.Client()
    columns_publish = config.COLUMNS_PUBLISH if hasattr(config, 'COLUMNS_PUBLISH') else None
    rows_result = []
    for new_chunk in [new_data[i:i+DATASTORE_CHUNK_SIZE] for i in range(0, len(new_data), DATASTORE_CHUNK_SIZE)]:
        new_state_items = {}
        for item in new_chunk:
            item_to_publish = gather_publish_msg(item, columns_publish)
            new_state_items[item_to_publish[state_storage_specification['id_property']]] = item_to_publish
        keys = [ds_client.key(state_storage_specification['entity_name'], key) for key in new_state_items.keys()]
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
    batch_message_size = config.BATCH_MESSAGE_SIZE if hasattr(config, 'BATCH_MESSAGE_SIZE') else None

    if not prefix_filter or filename.startswith(prefix_filter):
        try:
            # Read dataframe from store
            new_data = data_from_store(bucket, filename)
            state_storage_specification = config.STATE_STORAGE_SPECIFICATION

            if state_storage_specification['type'] == 'datastore':
                rows_json = calculate_diff_from_datastore(new_data, state_storage_specification)
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
                message_batch = []
                for publish_message in rows_json:
                    if not batch_message_size:
                        publish_json(publish_message, rowcount=i, rowmax=len(rows_json), **config.TOPIC_SETTINGS)
                    else:
                        message_batch.append(publish_message)
                        if len(message_batch) == batch_message_size:
                            publish_json(message_batch, rowcount=i, rowmax=len(rows_json),
                                         **config.TOPIC_SETTINGS)
                            message_batch = []

                    i += 1

                    if state_storage_specification['type'] == 'datastore':
                        datastore_new_state[publish_message[state_storage_specification['id_property']]] = publish_message
                        if len(datastore_new_state) > DATASTORE_CHUNK_SIZE:
                            store_to_datastore(datastore_new_state, state_storage_specification)
                            datastore_new_state = {}

                if message_batch:
                    publish_json(message_batch, rowcount=i, rowmax=len(rows_json),
                                 **config.TOPIC_SETTINGS)
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
    publish_diff({'bucket': config.INBOX, 'name': 'testfile.atom'}, {'event_id': 0, 'event_type': 'none'})
