import io
import logging
import time
import traceback

import config
import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO)


def send_bytestream_to_filestore(bytes_io, filename, bucket_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = storage.Blob(filename, bucket)
    blob.upload_from_string(bytes_io.getvalue(), content_type=config.MEDIA_TYPE)
    logging.info("Write file {} to {}".format(filename, bucket_name))


def preprocessing(file):
    logging.info("Preprocess start")
    try:
        df = pd.read_excel(
            file, converters={i: str for i in range(len(config.COLUMN_MAPPING.keys()))}
        )
    except Exception:
        df = pd.read_excel(file)
        message = """The file cannot be read by Python.
                     The uploaded file does not contain the correct columns.
                     The following ones are missing: {}""".format(
            ", ".join(
                list(set([i for i in config.COLUMN_MAPPING.keys()]) - set(list(df)))
            )
        )
        logging.info(message)
        traceback.print_exc()
        return dict(status="failure", message=message)

    # Check if contains the right columns
    if set(list(df)) != set(config.COLUMN_MAPPING.keys()):
        cols_expected = set([i for i in config.COLUMN_MAPPING.keys()])
        cols_file = set(list(df))
        missing = cols_expected - cols_file
        message = ""
        if len(missing) > 0:
            message = "The uploaded file does not contain the correct columns. The following columns are missing: {}".format(
                ", ".join(list(missing))
            )
            logging.info(message)
        to_many = cols_file - cols_expected
        if len(to_many) > 0:
            message = "\n".join(
                [
                    message,
                    "The uploaded file does not contain the correct columns. The following columns are not expected: {}".format(
                        ", ".join(list(to_many))
                    ),
                ]
            )
            logging.info(message)
        return dict(status="failure", message=message)

    # Check if contains data
    if len(df) == 0:
        message = "The uploaded file does not contain content"
        logging.info(message)
        return dict(status="warning", message=message)

    df = df.rename(columns=config.COLUMN_MAPPING)

    # Only keep non-PII columns
    df = df[config.COLUMNS_NONPII]

    # Return Excel file as byte-stream
    bytes_io = io.BytesIO()
    excel_writer = pd.ExcelWriter(bytes_io, engine="xlsxwriter")
    df.to_excel(excel_writer, sheet_name="data", index=False)
    excel_writer.save()

    return dict(
        status="success", message="excel-file succesfully processed", file=bytes_io
    )


def file_upload(request):
    try:
        # Verify the media type: commented out for testing purposes
        # if config.MEDIA_TYPE != request.headers.get('Content-Type', ''):
        #     logging.info('Unsupported media type uploaded: {}'.format(
        #         request.headers.get('Content-Type', 'No media-type provided')))
        #     return 'Unsupported media type', 415
        # else:
        #     logging.info('Supported media type uploaded: {}'.format(request.headers.get('Content-Type')))

        # Get file from request
        file = request.files.get("file")
        if not file:
            return "No file uploaded", 400
        else:
            logging.info("File uploaded: {}".format(file.filename))

        filename = "{}_{}_upload.xlsx".format(
            str(int(time.time())),
            config.TOPIC_SETTINGS["topic_name"],
        )
        preprocessed = preprocessing(file)
        if preprocessed["status"] == "success":
            send_bytestream_to_filestore(preprocessed["file"], filename, config.INBOX)
            return "OK", 200
        else:
            return preprocessed["status"] + ": " + preprocessed["message"], 400

    except Exception as e:
        return "Bad request: {}\n".format(e), 400
