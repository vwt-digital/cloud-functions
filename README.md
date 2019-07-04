# Event-sourcing-helpers
Collection of event sourcing helpers.

* http_file_upload
    * A Google Cloud function that accepts a file and posts it to a Google Cloud storage.

* produce_delta_event
    * A Google Cloud function that compares two pandas dataframes and posts the difference in rows (right-join) to a pub/sub topic.

* file_processing
    * A Google Cloud function that processes a file and moves it from one bucket to another.
