# Event-sourcing-helpers
Collection of event sourcing helpers.

* file_upload
A Google Cloud function that accepts a file and posts it to a Google Cloud storage.

* produce_delta_event
A Google Cloud function that compares two pandas dataframes and posts the difference in rows (right-join) to a pub/sub topic.
