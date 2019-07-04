import json
import config
import datetime
import pandas as pd
from io import BytesIO
from google.cloud import storage

logging.basicConfig(level=logging.INFO)


def read_bytestream_from_filestore(filename, bucket_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    bytesIO = BytesIO()
    blob.download_to_file(bytesIO)
    file = bytesIO.getvalue()
    logging.info('Read file {} from {}'.format(filename, bucket_name))
    return file


def send_bytestream_to_filestore(bytesIO, filename, bucket_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = storage.Blob(filename, bucket)
    blob.upload_from_string(
        bytesIO.getvalue(),
        content_type=config.MEDIA_TYPE
    )
    logging.info('Write file {} to {}'.format(filename, bucket_name))


def remove_file_from_filestore(filename, bucket_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.delete()
    logging.info('Deleted file {} from {}'.format(filename, bucket_name))


def preprocessing(file):
    logging.info('Preprocess start')
    df = pd.read_excel(file, converters={i: str for i in range(len(config.COLUMN_MAPPING.keys()))})

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

    # Add potency based on filename
    if 'potentieel' in file.filename.lower():
        df['potency_status'] = 'potentieel'
    else:
        df['potency_status'] = 'definitief'

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


def file_processing(data, context):
    logging.info('Run started')
    bucket = data['bucket']
    filename = data['name']

    try:
        # Read dataframe from store
        file = read_bytestream_from_filestore(bucket, filename)
        preprocessed = preprocessing(file)

        new_filename = '{}_{}_upload.xlsx'.format(
            time.time(),
            config.TOPIC_NAME,
        )

        send_bytestream_to_filestore(preprocessed['file'], new_filename, config.INBOX)
        remove_file_from_filestore(bucket, filename)

        logging.info('Processing file {} successful'.format(filename))
    except Exception as e:
        logging.error('Processing file {} failed!'.format(filename))
