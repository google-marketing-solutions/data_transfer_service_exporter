-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.campaign_stats_daily_{CUSTOMER_ID}`
AS
SELECT
  campaign_id,
  segments_date,
  metrics_impressions AS impressions,
  metrics_clicks AS clicks,
  metrics_cost_micros / 1000000 AS cost
FROM
  `{PROJECT_ID}.{DATASET_ID}.sa_CampaignStats_{CUSTOMER_ID}`;

CREATE OR REPLACE MODEL `{PROJECT_ID}.{DATASET_ID}.campaign_anomaly_detections_{CUSTOMER_ID}`
OPTIONS (
  model_type = 'ARIMA_PLUS_XREG',
  auto_arima = TRUE,
  time_series_data_col = 'impressions',
  time_series_timestamp_col = 'segments_date',
  horizon = 30,
  decompose_time_series = TRUE
)
AS
SELECT
  segments_date,
  impressions,
  clicks,
  cost
FROM
  `{PROJECT_ID}.{DATASET_ID}.campaign_stats_daily_{CUSTOMER_ID}`
-- Optional: filter by campaign_id
-- WHERE
--   campaign_id = {CAMPAIGN_ID}
ORDER BY segments_date;

SELECT *
FROM
  ML.DETECT_ANOMALIES(
    model `{PROJECT_ID}.{DATASET_ID}.campaign_anomaly_detections_{CUSTOMER_ID}`,
    STRUCT(0.6 AS anomaly_prob_threshold)
  )
ORDER BY
  segments_date;
