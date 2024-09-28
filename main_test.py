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

"""Testing the main module."""

import datetime
import os
from unittest import mock

from main import DataTransferServiceProcessor
import pytest


@pytest.fixture(name="mock_environment")
def fixture_mock_environment(monkeypatch):
  """Set up the environment variables."""
  monkeypatch.setenv("EXPORT_BUCKET_URI", "gs://test-bucket/")
  monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture(name="mock_bq_client")
def fixture_mock_bq_client():
  """Mock BigQuery client to prevent real API calls."""
  with mock.patch("main.bigquery.Client") as mock_client:
    yield mock_client


@pytest.fixture(name="mock_logging_client")
def fixture_mock_logging_client():
  """Mock Google Cloud logging client to prevent real logging setup."""
  with mock.patch("main.google.cloud.logging.Client") as mock_client:
    yield mock_client


@pytest.fixture(name="processor")
def fixture_processor(
    mock_environment,  # noqa: ARG001 pylint: disable=unused-argument
    mock_bq_client,  # noqa: ARG001 pylint: disable=unused-argument
    mock_logging_client,  # noqa: ARG001 pylint: disable=unused-argument
):
  """Returns a processor instance with mocked dependencies."""
  return DataTransferServiceProcessor()


def test_init(processor):
  """Test the initialization of the processor."""
  assert processor.export_bucket_uri == "gs://test-bucket/"
  assert processor.log_level == "DEBUG"


def test_setup_bq_client(mock_bq_client, processor):
  """Test the BigQuery client setup when in a Cloud environment."""
  os.environ["K_SERVICE"] = "test_service"
  bq_client = (
      processor._setup_bq_client()  # noqa: SLF001 pylint: disable=protected-access
  )
  mock_bq_client.assert_called_once()
  assert bq_client is not None


def test_setup_logging_cloud_env(mock_logging_client, processor):
  """Test logging setup in a Cloud environment."""
  os.environ["K_SERVICE"] = "test_service"
  logger = (
      processor._setup_logging()  # noqa: SLF001 pylint: disable=protected-access
  )
  mock_logging_client.assert_called()
  assert logger is not None


def test_run_sql_file(processor):
  """Test running an SQL file."""
  processor.bq_client = mock.MagicMock()
  mock_query = mock.MagicMock()
  processor.bq_client.query.return_value = mock_query

  with mock.patch(
      "builtins.open", mock.mock_open(read_data="SELECT * FROM test_table")
  ):
    processor.run_sql_file("test.sql", {"RUN_DATE": "2024-01-01"})

  processor.bq_client.query.assert_called_once_with(
      "SELECT * FROM test_table", job_id_prefix="dts_to_parquet_"
  )
  mock_query.result.assert_called_once()


def test_extract_run_date(processor):
  """Test run date extraction."""
  data = {"runTime": "2024-09-01T12:34:56Z"}
  run_date = processor.extract_run_date(data)
  assert run_date == datetime.date(2024, 9, 1)


def test_extract_project_id(processor):
  """Test Google Cloud Project ID extraction."""
  data = {"name": "projects/test_project/locations/us"}
  project_id = processor.extract_project_id(data)
  assert project_id == "test_project"


def test_extract_dataset_id(processor):
  """Test dataset ID extraction."""
  data = {"destinationDatasetId": "test_dataset"}
  dataset_id = processor.extract_dataset_id(data)
  assert dataset_id == "test_dataset"


def test_create_common_substitutions(processor):
  """Test creating common substitutions."""
  data = {
      "runTime": "2024-09-01T12:34:56Z",
      "name": "projects/test_project/locations/us",
      "destinationDatasetId": "test_dataset",
  }
  substitutions = processor.create_common_substitutions(data)
  assert substitutions == {
      "EXPORT_BUCKET_URI": "gs://test-bucket/",
      "PROJECT_ID": "test_project",
      "DATASET_ID": "test_dataset",
      "RUN_DATE": datetime.date(2024, 9, 1),
  }


@mock.patch("main.DataTransferServiceProcessor.run_google_ads")
@mock.patch("main.DataTransferServiceProcessor.run_search_ads")
@mock.patch("main.DataTransferServiceProcessor.run_displayvideo")
@mock.patch("main.DataTransferServiceProcessor.run_dcm_dt")
def test_process_data(
    mock_run_dcm_dt,
    mock_run_displayvideo,
    mock_run_search_ads,
    mock_run_google_ads,
    processor,
):
  """Test processing of data for different data sources."""
  # Test Google Ads
  data = {
      "dataSourceId": "google_ads",
      "runTime": "2024-09-01T12:34:56Z",
      "name": "projects/test_project/locations/us",
      "destinationDatasetId": "test_dataset",
      "params": {"customer_id": "123-456-7890"},
  }
  processor.process_data(data)
  mock_run_google_ads.assert_called_once()

  # Test Search Ads
  data["dataSourceId"] = "search_ads"
  processor.process_data(data)
  mock_run_search_ads.assert_called_once()

  # Test Display & Video 360
  data["dataSourceId"] = "displayvideo"
  data["params"]["displayvideo_id"] = "123456789"
  processor.process_data(data)
  mock_run_displayvideo.assert_called_once()

  # Test Campaign Manager
  data["dataSourceId"] = "dcm_dt"
  data["params"]["network_id"] = "123456789"
  processor.process_data(data)
  mock_run_dcm_dt.assert_called_once()

  # Test unimplemented data source
  with pytest.raises(NotImplementedError) as exc_info:
    data["dataSourceId"] = "unknown_source"
    processor.process_data(data)

  assert (
      str(exc_info.value)
      == "Data source not implemented: {'dataSourceId': 'unknown_source',"
      " 'runTime': '2024-09-01T12:34:56Z', 'name':"
      " 'projects/test_project/locations/us', 'destinationDatasetId':"
      " 'test_dataset', 'params': {'customer_id': '123-456-7890',"
      " 'displayvideo_id': '123456789', 'network_id': '123456789'}}"
  )


def test_process_data_error(processor):
  """Test processing of data with an error status."""
  data = {
      "dataSourceId": "google_ads",
      "params": {"customer_id": "123-456-7890"},
      "errorStatus": "Something went wrong",
  }

  with pytest.raises(ValueError) as exc_info:
    processor.process_data(data)

  assert (
      str(exc_info.value)
      == "Error in processing: {'dataSourceId': 'google_ads', 'params':"
      " {'customer_id': '123-456-7890'}, 'errorStatus': 'Something went"
      " wrong'}"
  )
