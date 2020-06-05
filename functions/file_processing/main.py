import json
import config
import logging
import traceback
import io
import time
import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
client = storage.Client()


def send_bytestream_to_filestore(bytesIO, filename, bucket_name):
    bucket = client.get_bucket(bucket_name)
    blob = storage.Blob(filename, bucket)
    if filename.endswith('.xlsx'):
        blob.upload_from_string(
            bytesIO.getvalue(),
            content_type=config.MEDIA_TYPE
        )
    else:
        blob.upload_from_string(
            bytesIO,
            content_type=config.MEDIA_TYPE
        )
    logging.info('Write file {} to {}'.format(filename, bucket_name))


def remove_file_from_filestore(bucket_name, filename):
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.delete()
    logging.info('Deleted file {} from {}'.format(filename, bucket_name))


def preprocessing(bucket_name, blob_name):
    logging.info('Preprocess start')
    df = df_from_store(bucket_name, blob_name)

    # Check if contains the correct columns
    cols_exp = set(list(config.COLUMN_MAPPING.keys()))
    cols_present = set(list(df))
    if cols_exp != cols_present:
        # Create error message
        to_many = cols_present - cols_exp
        missing = cols_exp - cols_present
        message = 'The uploaded file does not contain the correct columns.'
        if len(to_many) != 0:
            message = message + ' The following columns are abundant: "{}".'.format(
                '", "'.join(list(to_many)))
        if len(missing) != 0:
            message = message + ' The following columns are missing: "{}".'.format(
                '", "'.join(list(missing)))

        logging.info(message)
        return dict(
            status='failure',
            message=message
        )

    # Check if contains data
    if len(df) == 0:
        message = 'The uploaded file does not contain content'
        logging.info(message)
        return dict(
            status='warning',
            message=message
        )

    df = df.rename(columns=config.COLUMN_MAPPING)

    # Only keep non-PII columns
    df = df[config.COLUMNS_NONPII]

    # Return file as byte-stream
    if blob_name.endswith('.xlsx'):
        bytesIO = io.BytesIO()
        excel_writer = pd.ExcelWriter(bytesIO, engine="xlsxwriter")
        df.to_excel(excel_writer, sheet_name="data", index=False)
        excel_writer.save()
    else:
        bytesIO = df.to_json()

    return dict(
        status='success',
        message='file succesfully processed',
        file=bytesIO
    )


def df_from_store(bucket_name, blob_name):
    path = 'gs://{}/{}'.format(bucket_name, blob_name)
    if blob_name.endswith('.xlsx'):
        df = pd.read_excel(path, dtype=str)
    elif blob_name.endswith('.json'):
        if hasattr(config, 'JSON_ELEMENTS'):
            json_elements = getattr(config, 'JSON_ELEMENTS', [])
            bucket = storage.Client().get_bucket(bucket_name)
            blob = storage.Blob(blob_name, bucket)
            content = blob.download_as_string()
            data = json.loads(content.decode('utf-8'))
            for el in json_elements:
                data = data[el]
            df = pd.DataFrame.from_records(data)
        else:
            df = pd.read_json(path, dtype=False).to_dict(orient='records')
    else:
        raise ValueError('File is not json or xlsx: {}'.format(blob_name))
    logging.info('Read file {} from {}'.format(blob_name, bucket_name))
    return df


def file_processing(data, context):
    logging.info('Run started')
    bucket_name = data['bucket']
    filename = data['name']

    try:
        # Read dataframe from store
        preprocessed = preprocessing(bucket_name, filename)

        if filename.endswith('.xlsx'):
            new_filename = '{}_{}_upload.xlsx'.format(
                str(int(time.time())),
                config.TOPIC_SETTINGS['topic_name'],
            )
        else:
            new_filename = '{}_{}_upload.json'.format(
                str(int(time.time())),
                config.TOPIC_SETTINGS['topic_name'],
            )

        send_bytestream_to_filestore(preprocessed['file'], new_filename, config.INBOX)
        delete = config.DELETE if hasattr(config, 'DELETE') else True
        if delete:
            remove_file_from_filestore(bucket_name, filename)

        logging.info('Processing file {} successful'.format(filename))
    except Exception:
        logging.error('Processing file {} failed!'.format(filename))
        traceback.print_exc()
