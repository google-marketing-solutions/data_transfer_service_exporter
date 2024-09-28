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
  WHERE table_id = 'p_Impression_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/Impression/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_Impression_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_Click_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/Click/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_Click_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_Activity_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/Activity/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_Activity_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_LineItem_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/LineItem/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_LineItem_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_LineItemTargeting_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/LineItemTargeting/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_LineItemTargeting_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_Campaign_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/Campaign/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_Campaign_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_CampaignTargeting_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/CampaignTargeting/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_CampaignTargeting_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_InsertionOrder_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/InsertionOrder/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_InsertionOrder_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_InsertionOrderTargeting_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/InsertionOrderTargeting/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_InsertionOrderTargeting_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_AdGroup_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/AdGroup/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_AdGroup_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_AdGroupTargeting_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/AdGroupTargeting/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_AdGroupTargeting_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_Partner_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/Partner/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_Partner_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_Advertiser_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/Advertiser/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_Advertiser_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_Creative_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/Creative/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_Creative_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

-- AdGroupAd requires a few fields that are JSON. We will remove them for exporting as Parquet since that is not supported.
IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_AdGroupAd_{DISPLAYVIDEO_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/AdGroupAd/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      * REPLACE (
        (
          SELECT AS STRUCT instreamad.* EXCEPT (customparameters)
        ) AS instreamad,
        (
          SELECT AS STRUCT nonskippablead.* EXCEPT (customparameters)
        ) AS nonskippablead,
        (
          SELECT AS STRUCT videoperformancead.* EXCEPT (customparameters)
        ) AS videoperformancead
      ),
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_AdGroupAd_{DISPLAYVIDEO_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

EXPORT DATA OPTIONS (
  uri = '{EXPORT_BUCKET_URI}/displayvideo/{DISPLAYVIDEO_ID}/{RUN_DATE}/Done/*.parquet',
  format = 'PARQUET',
  overwrite = TRUE
) AS (
  SELECT
    TRUE AS done,
    CURRENT_DATE() AS _LATEST_DATE,
    DATE('{RUN_DATE}') AS _DATA_DATE
);
