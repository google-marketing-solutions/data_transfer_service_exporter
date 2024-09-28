# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cloud Function to export Data Transfer Service data."""

import base64
import datetime
import json
import logging
import os
import sys
from typing import Any

from cloudevents.http import CloudEvent
import functions_framework
from google.cloud import bigquery
import google.cloud.logging


class DataTransferServiceProcessor:
  """A class to process Data Transfer Service data."""

  SQL_FILES = {
      "google_ads": "sql/google_ads.sql",
      "search_ads": "sql/search_ads.sql",
      "displayvideo": "sql/displayvideo.sql",
      "dcm_dt": "sql/dcm_dt.sql",
  }

  GOOGLE_ADS = "google_ads"
  SEARCH_ADS = "search_ads"
  DISPLAYVIDEO = "displayvideo"
  DCM_DT = "dcm_dt"

  def __init__(self) -> None:
    """Initialize with environment variables, logging, and a BigQuery client."""
    self.export_bucket_uri = os.getenv(
        "EXPORT_BUCKET_URI", "gs://datawarehouse/"
    )
    self.log_level = os.getenv("LOG_LEVEL", "INFO")
    self.bq_client = self._setup_bq_client()
    self.logger = self._setup_logging()

  def _setup_bq_client(self) -> bigquery.Client | None:
    """Sets up the BigQuery client."""
    if os.getenv("K_SERVICE"):
      return bigquery.Client()
    return None

  def _setup_logging(self) -> logging.Logger:
    """Sets up logging depending on the environment."""
    if os.getenv("K_SERVICE"):
      logging_client = google.cloud.logging.Client()
      logging_client.setup_logging()
    else:
      logging.basicConfig()

    logger = logging.getLogger(__name__)
    logger.setLevel(self.log_level)
    return logger

  def run_sql_file(self, path: str, subs: dict[str, Any]) -> None:
    """Executes a SQL file with provided substitutions."""
    with open(path, encoding="utf-8") as sql_file:
      query = sql_file.read().format(**subs)
    job = self.bq_client.query(  # pytype: disable=attribute-error
        query,
        job_id_prefix="dts_to_parquet_",
    )
    job.result()

  def extract_run_date(self, data: dict[str, Any]) -> datetime.date:
    """Extracts the run date from the data."""
    return datetime.datetime.fromisoformat(data["runTime"]).date()

  def extract_project_id(self, data: dict[str, Any]) -> str:
    """Extracts the Google Cloud Project ID from the data."""
    return data["name"].split("/")[1]

  def extract_dataset_id(self, data: dict[str, Any]) -> str:
    """Extracts the Dataset ID from the data."""
    return data["destinationDatasetId"]

  def extract_customer_id(self, data: dict[str, Any]) -> str:
    """Extracts and sanitizes the Customer ID."""
    return data["params"].get("customer_id", "").replace("-", "")

  def create_common_substitutions(self, data: dict[str, Any]) -> dict[str, Any]:
    """Creates common substitutions for SQL queries."""
    return {
        "EXPORT_BUCKET_URI": self.export_bucket_uri,
        "PROJECT_ID": self.extract_project_id(data),
        "DATASET_ID": self.extract_dataset_id(data),
        "RUN_DATE": self.extract_run_date(data),
    }

  def run_google_ads(self, data: dict[str, Any]) -> None:
    """Processes Google Ads data."""
    subs = {
        **self.create_common_substitutions(data),
        "CUSTOMER_ID": self.extract_customer_id(data),
    }
    self.run_sql_file(self.SQL_FILES[self.GOOGLE_ADS], subs)

  def run_search_ads(self, data: dict[str, Any]) -> None:
    """Processes Search Ads 360 data."""
    subs = {
        **self.create_common_substitutions(data),
        "CUSTOMER_ID": self.extract_customer_id(data),
    }
    self.run_sql_file(self.SQL_FILES[self.SEARCH_ADS], subs)

  def run_displayvideo(self, data: dict[str, Any]) -> None:
    """Processes Display & Video 360 data."""
    subs = {
        **self.create_common_substitutions(data),
        "DISPLAYVIDEO_ID": data["params"].get("displayvideo_id", ""),
    }
    self.run_sql_file(self.SQL_FILES[self.DISPLAYVIDEO], subs)

  def run_dcm_dt(self, data: dict[str, Any]) -> None:
    """Processes Campaign Manager 360 data."""
    subs = {
        **self.create_common_substitutions(data),
        "NETWORK_ID": data["params"].get("network_id", ""),
    }
    self.run_sql_file(self.SQL_FILES[self.DCM_DT], subs)

  def process_data(self, data: dict[str, Any]) -> None:
    """Processes the received Data Transfer Service data."""
    if not hasattr(self, "logger"):
      self.logger = logging.getLogger(__name__)
      self.logger.setLevel(logging.INFO)

    self.logger.info(
        "Received pub/sub notification.", extra={"json_fields": data}
    )

    if data.get("errorStatus"):
      error_message = f"Error in processing: {data}"

      self.logger.error(error_message)
      raise ValueError(error_message)

    data_source_handlers = {
        self.SEARCH_ADS: self.run_search_ads,
        self.GOOGLE_ADS: self.run_google_ads,
        self.DISPLAYVIDEO: self.run_displayvideo,
        self.DCM_DT: self.run_dcm_dt,
    }

    handler = data_source_handlers.get(data["dataSourceId"])
    if handler:
      handler(data)
      self.logger.info("Loaded data.", extra={"json_fields": data})
    else:
      error_message = f"Data source not implemented: {data}"
      self.logger.error(error_message)
      raise NotImplementedError(error_message)


@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent) -> None:
  """Cloud Function entry point for Pub/Sub messages."""
  client = DataTransferServiceProcessor()
  data = json.loads(base64.b64decode(cloud_event.data["message"]["data"]))
  client.process_data(data)


if __name__ == "__main__":
  if len(sys.argv) < 2:
    sys.exit("Filename for JSON input is required.")
  with open(sys.argv[1], encoding="utf-8") as json_file:
    processor = DataTransferServiceProcessor()
    processor.process_data(json.load(json_file))
