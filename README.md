# cf_gcs_csv_to_bq
When GCS csv finalize, trigger to BigQuery in using Golang on Cloud Functions

## Editor Config
copy .env.example.yaml to .env.yaml
```
$ cp .env.example.yaml .env.yaml
```

```
$ vim ./.env.yaml
```
Setup your BQ project_id, dataset and table name

## Deploy
```
$ gcloud functions deploy GCSToBigQuery --runtime go111 --trigger-resource <YOUR_BUCKET_NAME> --trigger-event google.storage.object.finalize --env-vars-file .env.yaml
```
