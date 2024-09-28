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
  WHERE table_id = 'p_ads_AccountBasicStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AccountBasicStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AccountBasicStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AccountConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AccountConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AccountConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AccountNonClickStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AccountNonClickStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AccountNonClickStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AccountStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AccountStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AccountStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdBasicStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdBasicStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdBasicStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdCrossDeviceConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdCrossDeviceConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdCrossDeviceConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdCrossDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdCrossDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdCrossDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupAdLabel_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupAdLabel/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupAdLabel_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupAudienceBasicStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupAudienceBasicStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupAudienceBasicStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupAudienceConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupAudienceConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupAudienceConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupAudienceNonClickStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupAudienceNonClickStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupAudienceNonClickStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupAudienceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupAudienceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupAudienceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupAudience_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupAudience/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupAudience_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupBasicStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupBasicStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupBasicStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupBidModifier_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupBidModifier/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupBidModifier_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupCriterionLabel_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupCriterionLabel/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupCriterionLabel_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupCriterion_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupCriterion/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupCriterion_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupCrossDeviceConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupCrossDeviceConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupCrossDeviceConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupCrossDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupCrossDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupCrossDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupLabel_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupLabel/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupLabel_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroupStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroupStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdGroup_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroup/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdGroup_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AdStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AdStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AdStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_Ad_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/Ad/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_Ad_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AgeRangeBasicStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AgeRangeBasicStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AgeRangeBasicStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AgeRangeConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AgeRangeConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AgeRangeConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AgeRangeNonClickStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AgeRangeNonClickStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AgeRangeNonClickStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AgeRangeStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AgeRangeStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AgeRangeStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_AgeRange_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/AgeRange/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_AgeRange_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_BidGoalConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/BidGoalConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_BidGoalConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_BidGoalStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/BidGoalStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_BidGoalStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_BidGoal_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/BidGoal/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_BidGoal_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_BudgetStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/BudgetStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_BudgetStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_Budget_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/Budget/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_Budget_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignAudienceBasicStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignAudienceBasicStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignAudienceBasicStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignAudienceConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignAudienceConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignAudienceConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignAudienceNonClickStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignAudienceNonClickStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignAudienceNonClickStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignAudienceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignAudienceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignAudienceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignAudience_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignAudience/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignAudience_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignBasicStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignBasicStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignBasicStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignCookieStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignCookieStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignCookieStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignCriterion_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignCriterion/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignCriterion_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignCrossDeviceConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignCrossDeviceConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignCrossDeviceConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignCrossDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignCrossDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignCrossDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignLabel_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignLabel/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignLabel_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignLocationTargetStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignLocationTargetStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignLocationTargetStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_CampaignStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_CampaignStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_Campaign_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/Campaign/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_Campaign_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_ClickStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/ClickStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_ClickStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_Customer_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/Customer/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_Customer_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_GenderBasicStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/GenderBasicStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_GenderBasicStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_GenderConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/GenderConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_GenderConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_GenderNonClickStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/GenderNonClickStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_GenderNonClickStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_GenderStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/GenderStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_GenderStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_Gender_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/Gender/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_Gender_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_GeoConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/GeoConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_GeoConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_GeoStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/GeoStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_GeoStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_HourlyAccountConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/HourlyAccountConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_HourlyAccountConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_HourlyAccountStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/HourlyAccountStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_HourlyAccountStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_HourlyAdGroupConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/HourlyAdGroupConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_HourlyAdGroupConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_HourlyAdGroupStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/HourlyAdGroupStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_HourlyAdGroupStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_HourlyBidGoalStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/HourlyBidGoalStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_HourlyBidGoalStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_HourlyCampaignConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/HourlyCampaignConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_HourlyCampaignConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_HourlyCampaignStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/HourlyCampaignStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_HourlyCampaignStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_KeywordBasicStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/KeywordBasicStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_KeywordBasicStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_KeywordConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/KeywordConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_KeywordConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_KeywordCrossDeviceConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/KeywordCrossDeviceConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_KeywordCrossDeviceConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_KeywordCrossDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/KeywordCrossDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_KeywordCrossDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_KeywordStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/KeywordStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_KeywordStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_Keyword_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/Keyword/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_Keyword_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_LocationBasedCampaignCriterion_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/LocationBasedCampaignCriterion/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_LocationBasedCampaignCriterion_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_PaidOrganicStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/PaidOrganicStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_PaidOrganicStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_ParentalStatusBasicStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/ParentalStatusBasicStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_ParentalStatusBasicStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_ParentalStatusConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/ParentalStatusConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_ParentalStatusConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_ParentalStatusNonClickStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/ParentalStatusNonClickStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_ParentalStatusNonClickStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_ParentalStatusStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/ParentalStatusStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_ParentalStatusStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_ParentalStatus_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/ParentalStatus/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_ParentalStatus_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_PlacementBasicStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/PlacementBasicStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_PlacementBasicStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_PlacementConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/PlacementConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_PlacementConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_PlacementNonClickStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/PlacementNonClickStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_PlacementNonClickStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_PlacementStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/PlacementStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_PlacementStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_Placement_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/Placement/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_Placement_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_ProductGroupStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/ProductGroupStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_ProductGroupStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_SearchQueryConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/SearchQueryConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_SearchQueryConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_SearchQueryStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/SearchQueryStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_SearchQueryStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_ShoppingProductConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/ShoppingProductConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_ShoppingProductConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_ShoppingProductStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/ShoppingProductStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_ShoppingProductStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_VideoBasicStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/VideoBasicStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_VideoBasicStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_VideoConversionStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/VideoConversionStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_VideoConversionStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_VideoNonClickStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/VideoNonClickStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_VideoNonClickStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_VideoStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/VideoStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_VideoStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_ads_Video_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/Video/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_ads_Video_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

EXPORT DATA OPTIONS (
  uri = '{EXPORT_BUCKET_URI}/google_ads/{CUSTOMER_ID}/{RUN_DATE}/Done/*.parquet',
  format = 'PARQUET',
  overwrite = TRUE
) AS (
  SELECT
    TRUE AS done,
    CURRENT_DATE() AS _LATEST_DATE,
    DATE('{RUN_DATE}') AS _DATA_DATE
);
