gcloud functions deploy ${PROJECT_ID}-producer-func \
  --entry-point=handler \
  --runtime=python37 \
  --trigger-http \
  --project=${PROJECT_ID} \
  --region=europe-west1 \
  --timeout=540s \
  --memory=2048MB
