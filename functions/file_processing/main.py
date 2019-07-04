import json
import config
import datetime
import logging
import traceback
import io
import time
import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO)


def send_bytestream_to_filestore(bytesIO, filename, bucket_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = storage.Blob(filename, bucket)
    blob.upload_from_string(
        bytesIO.getvalue(),
        content_type=config.MEDIA_TYPE
    )
    logging.info('Write file {} to {}'.format(filename, bucket_name))


def remove_file_from_filestore(bucket_name, filename):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.delete()
    logging.info('Deleted file {} from {}'.format(filename, bucket_name))


def preprocessing(bucket_name, blob_name):
    logging.info('Preprocess start')
    df = df_from_store(bucket_name, blob_name)

    # Check if contains the right columns
    if set(list(df)) != set(config.COLUMN_MAPPING.keys()):
        message = 'The uploaded file does not contain the correct columns. The following ones are missing: {}'.format(
            ', '.join(list(set(config.COLUMN_MAPPING.keys()) - set(list(df)))))
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

    # Return Excel file as byte-stream
    bytesIO = io.BytesIO()
    excel_writer = pd.ExcelWriter(bytesIO, engine="xlsxwriter")
    df.to_excel(excel_writer, sheet_name="data", index=False)
    excel_writer.save()

    return dict(
        status='success',
        message='excel-file succesfully processed',
        file=bytesIO
    )

def df_from_store(bucket_name, blob_name):
    path = 'gs://{}/{}'.format(bucket_name, blob_name)
    if blob_name.endswith('.xlsx'):
        df = pd.read_excel(path, converters={i: str for i in range(len(config.COLUMN_MAPPING.keys()))})
    elif blob_name.endswith('.json'):
        df = pd.read_json(path, dtype=False)
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

        new_filename = '{}_{}_upload.xlsx'.format(
            str(int(time.time())),
            config.TOPIC_SETTINGS['topic_name'],
        )

        send_bytestream_to_filestore(preprocessed['file'], new_filename, config.INBOX)
        remove_file_from_filestore(bucket_name, filename)

        logging.info('Processing file {} successful'.format(filename))
    except Exception as e:
        logging.error('Processing file {} failed!'.format(filename))
        traceback.print_exc()
