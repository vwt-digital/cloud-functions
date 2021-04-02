# Topic to publish to
TOPIC_SETTINGS = {"topic_project_id": "my-topic-id", "topic_name": "my-topic-name"}

# Parameters added to pubsub_v1.types.BatchSettings to initialize pubsub PublisherClient
TOPIC_BATCH_SETTINGS = {"max_messages": 100}

# Parameters added to pandas.read_csv when reading csv file
CSV_DIALECT_PARAMETERS = {"sep": ",", "quotechar": '"'}

# Number of entries to send in a single message
BATCH_MESSAGE_SIZE = 100

# Storage buckets to use
# INBOX will be used to load input
# ARCHIVE will be used to store all handled input, input on INBOX will be removed when stored to ARCHIVE
# If INBOX and ARCHIVE are the same bucket, original input will remain on bucket
# ERROR is optional, when defined any invalid input file will be stored on ERROR. If INBOX and ARCHIVE are different
# buckets, then original input will be deleted when stored on ERROR.
INBOX = "my-inbox-bucket"
ARCHIVE = "my-archive-bucket"
ERROR = "my-error-bucket"

# Optional, if True then all items will be published, regardless changes to previous data
FULL_LOAD = False

# Optional, if False then no duplicate items are deleted.
SHOULD_DROP_DUPLICATES = True

# Dict that contains mapping from column names to the used column names
COLUMN_MAPPING = {
    "Example A": "example_A",
}

# List of columns without personal data
COLUMNS_NONPII = []

# List of columns to drop (for instance containing reporting date)
COLUMNS_DROP = []

# Number of entries to send in a single message
BATCH_MESSAGE_SIZE = 100

# Specification of attributes in published message. Key value will be key value in the published message,
# value in COLUMNS_PUBLISH will be attribute in source from which value of the respective field will be retrieved when
# using it as a string {[name]: [field]}. When using a dictionary
# {[name]: {'source_attribute': [field], 'conversion': [conversion]}}, the key 'source_attribute' will be the object
# from where the field will be fetched (can be a list to match first attribute). An optional key 'conversion' can be
# filled with one or a list of the following to convert the fetched value: 'lowercase', 'uppercase', 'capitalize', 'numeric',
# 'datetime' or 'geojson_point'. The 'datetime' format requires two extra fields, 'format_from' and 'format_to',
# to support conversion. The 'geojson_point' conversion requires longitude_attribute and latitude_attribute to be
# defined as extra fields. A GeoJSON Point body will be the value of the respective message attribute.
COLUMNS_PUBLISH = {
    "attribute_name_in_msg_1": {
        "source_attribute": "attribute_name_in_source_1",
        "conversion": "lowercase",
    },
    "attribute_name_in_msg_2": {
        "source_attribute": [
            "attribute_name_in_source_2_1",
            "attribute_name_in_source_2_2",
        ],
        "conversion": ["strip", "uppercase"],
    },
    "attribute_name_in_msg_3": {
        "source_attribute": "attribute_name_in_source_3",
        "conversion": "datetime",
        "format_from": "%Y-%m-%dT%H:%M:%SZ",
        "format_to": "%Y-%m-%dT%H:%M:%SZ",
    },
    "attribute_geometry": {
        "conversion": "geojson_point",
        "longitude_attribute": "longitude",
        "latitude_attribute": "latitude",
    },
    "name": "last_name",
}

# ATTRIBUTE_WITH_THE_LIST is used when reading data from JSON where the top-level is an object instead
# of a list. The list containing the data to load is the value of an attribute of the top-level object.
# For example, setting ATTRIBUTE_WITH_THE_LIST to rows, will load this JSON: {"rows": [{"field1": "value1"}] }
# If the JSON data top-level object is a list (like [{"field1": "value1"}]), then ATTRIBUTE_WITH_THE_LIST should
# not be declared.
ATTRIBUTE_WITH_THE_LIST = "rows"

# When FILEPATH_PREFIX_FILTER is defined, the publish_diff function will only handle files with a name that
# starts with the specified prefix. For example, when setting FILEPATH_PREFIX_FILTER to source/mydatadir, only
# files in the source/mydatadir will be handled.
FILEPATH_PREFIX_FILTER = "source/mydatadir"
