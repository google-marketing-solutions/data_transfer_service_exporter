**This is not an officially supported Google product.**

# Data Transfer Service Exporter

A Cloud Function to export data from the Data Transfer Service to Google Cloud
Storage in a compressed Parquet file format.

[![Continuous Integration](https://github.com/google-marketing-solutions/data_transfer_service_exporter/actions/workflows/ci.yml/badge.svg)](https://github.com/google-marketing-solutions/data_transfer_service_exporter/actions/workflows/ci.yml)
[![Code Style: Google](https://img.shields.io/badge/code%20style-google-4285F4.svg)](https://github.com/google/pyink)
[![Conventional Commits](https://img.shields.io/badge/conventional%20commits-1.0.0-fe5196.svg?logo=conventionalcommits)](https://conventionalcommits.org)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)

## Deployment

Start by initializing the Cloud SDK:

```shell
gcloud config set project <PROJECT_ID>
gcloud config set functions/region <LOCATION>
```

Enable the required API services:

```shell
gcloud services enable artifactregistry.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable bigquerydatatransfer.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable eventarc.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable searchads360.googleapis.com
gcloud services enable googleads.googleapis.com
gcloud services enable displayvideo.googleapis.com
gcloud services enable doubleclickbidmanager.googleapis.com
gcloud services enable dfareporting.googleapis.com
```

Set up a Pub/Sub topic for the Data Transfer Service configs to publish events
on and which triggers the Cloud Function. To do so, run:

```shell
gcloud pubsub topics create data-transfer-service-events
```

Deploy the Cloud Function:

```shell
gcloud functions deploy export_data_transfer_service \
  --entry-point=subscribe \
  --gen2 \
  --memory=256MB \
  --runtime=python312 \
  --timeout=540s \
  --trigger-topic=data-transfer-service-events
```

Create a Data Transfer Service transfer configuration. For example:

```shell
bq mk --transfer_config \
  --target_dataset=data_transfer_services \
  --display_name="Search Ads 360 - <CUSTOMER_ID>" \
  --params='{"customer_id":"'"<CUSTOMER_ID>"'"}' \
  --notification-pubsub-topic="projects/<PROJECT_ID>/topics/data-transfer-service-events" \
  --data_source=search_ads
```

## Development

Install the dependencies:

```shell
pip install -e '.[dev]'
```

### Testing

Run the tests:

```shell
pytest
```

### Linting

Lint the project:

```shell
pylint .
```

### Formatting

Format the project:

```shell
pyink --line-length=80 --pyink-indentation=2 --pyink-use-majority-quotes .
```

### Pre-Commit

Install the [pre-commit](https://pre-commit.com) hooks into `git`:

```shell
pre-commit install
```

All commits will be tested, linted, and formatted.

## Contributing

Want to contribute? [Learn more](docs/contributing.md)
