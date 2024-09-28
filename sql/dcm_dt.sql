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

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_activity_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/activity/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_activity_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_click_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/click/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_click_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_impression_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/impression/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_impression_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_rich_media_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/rich_media/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_rich_media_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_activity_cats_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_activity_cats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_activity_cats_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_activity_types_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_activity_types/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_activity_types_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_ad_placement_assignments_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_ad_placement_assignments/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_ad_placement_assignments_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_ads_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_ads/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_ads_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_advertisers_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_advertisers/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_advertisers_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_assets_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_assets/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_assets_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_browsers_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_browsers/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_browsers_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_campaigns_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_campaigns/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_campaigns_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_cities_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_cities/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_cities_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_creative_ad_assignments_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_creative_ad_assignments/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_creative_ad_assignments_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_creatives_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_creatives/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_creatives_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_custom_creative_fields_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_custom_creative_fields/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_custom_creative_fields_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_custom_floodlight_variables_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_custom_floodlight_variables/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_custom_floodlight_variables_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_custom_rich_media_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_custom_rich_media/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_custom_rich_media_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_designated_market_areas_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_designated_market_areas/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_designated_market_areas_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_keyword_value_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_keyword_value/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_keyword_value_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_landing_page_url_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_landing_page_url/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_landing_page_url_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_operating_systems_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_operating_systems/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_operating_systems_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_paid_search_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_paid_search/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_paid_search_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_placement_cost_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_placement_cost/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_placement_cost_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_placements_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_placements/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_placements_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_sites_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_sites/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_sites_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_match_table_states_{NETWORK_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/match_table_states/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_match_table_states_{NETWORK_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

EXPORT DATA OPTIONS (
  uri = '{EXPORT_BUCKET_URI}/dcm_dt/{NETWORK_ID}/{RUN_DATE}/done/*.parquet',
  format = 'PARQUET',
  overwrite = TRUE
) AS (
  SELECT
    TRUE AS done,
    CURRENT_DATE() AS _LATEST_DATE,
    DATE('{RUN_DATE}') AS _DATA_DATE
);
