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

WITH stats AS (
  SELECT
    campaign_id,
    segments_date,
    metrics_impressions,

    -- 7-day rolling mean and stddev
    AVG(metrics_impressions)
      OVER (
        PARTITION BY campaign_id
        ORDER BY segments_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) AS mean_7day,
    STDDEV(metrics_impressions)
      OVER (
        PARTITION BY campaign_id
        ORDER BY segments_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) AS stddev_7day,

      -- 30-day rolling mean and stddev
    AVG(metrics_impressions)
      OVER (
        PARTITION BY campaign_id
        ORDER BY segments_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ) AS mean_30day,
    STDDEV(metrics_impressions)
      OVER (
        PARTITION BY campaign_id
        ORDER BY segments_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ) AS stddev_30day
  FROM
    `{PROJECT_ID}.{DATASET_ID}.sa_CampaignStats_{CUSTOMER_ID}`
-- Optional: filter by campaign_id
-- WHERE
--   campaign_id = 123456789
),

anomalies AS (
  SELECT
    campaign_id,
    segments_date,
    metrics_impressions,
    mean_7day,
    stddev_7day,
    mean_30day,
    stddev_30day,

    -- Detect anomalies based on the 30-day window
    COALESCE(
      (metrics_impressions < mean_30day - 2 * stddev_30day)
      OR (metrics_impressions > mean_30day + 2 * stddev_30day), FALSE
    ) AS is_anomaly
  FROM
    stats
)

SELECT
  campaign_id,
  segments_date,
  metrics_impressions,

  -- 7-day bands
  is_anomaly,
  GREATEST(mean_7day - stddev_7day, 0) AS lower_bound_1std_7day,
  mean_7day + stddev_7day AS upper_bound_1std_7day,
  GREATEST(mean_7day - 2 * stddev_7day, 0) AS lower_bound_2std_7day,

  -- 30-day bands
  mean_7day + 2 * stddev_7day AS upper_bound_2std_7day,
  GREATEST(mean_30day - stddev_30day, 0) AS lower_bound_1std_30day,
  mean_30day + stddev_30day AS upper_bound_1std_30day,
  GREATEST(mean_30day - 2 * stddev_30day, 0) AS lower_bound_2std_30day,

  mean_30day + 2 * stddev_30day AS upper_bound_2std_30day
FROM
  anomalies
ORDER BY campaign_id, segments_date;
