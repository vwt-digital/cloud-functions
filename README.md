# Cloud-functions
Collection of google cloud functions.

* file_upload

This function accepts a file and posts it to a Google Cloud storage.

* produce_delta_event

This function compares two pandas dataframes and posts the difference in rows (right-join) to a pub/sub topic.
