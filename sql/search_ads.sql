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
  WHERE table_id = 'p_sa_KeywordDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/KeywordDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_KeywordDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_Account_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/Account/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_Account_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AccountAssetStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AccountAssetStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AccountAssetStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AccountConversionActionAndAssetStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AccountConversionActionAndAssetStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AccountConversionActionAndAssetStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AccountConversionActionAndDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AccountConversionActionAndDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AccountConversionActionAndDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AccountDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AccountDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AccountDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AccountStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AccountStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AccountStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_Ad_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/Ad/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_Ad_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AdConversionActionAndDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AdConversionActionAndDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AdConversionActionAndDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AdDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AdDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AdDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AdGroup_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroup/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AdGroup_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AdGroupAssetStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupAssetStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AdGroupAssetStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AdGroupAudienceConversionActionAndDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupAudienceConversionActionAndDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AdGroupAudienceConversionActionAndDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AdGroupAudienceDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupAudienceDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AdGroupAudienceDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AdGroupConversionActionAndAssetStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupConversionActionAndAssetStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AdGroupConversionActionAndAssetStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AdGroupConversionActionAndDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupConversionActionAndDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AdGroupConversionActionAndDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AdGroupCriterion_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupCriterion/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AdGroupCriterion_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AdGroupDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AdGroupDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AdGroupStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AdGroupStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AdGroupStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AgeRangeConversionActionAndDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AgeRangeConversionActionAndDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AgeRangeConversionActionAndDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AgeRangeDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AgeRangeDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AgeRangeDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_Asset_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/Asset/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_Asset_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_AssetSetStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/AssetSetStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_AssetSetStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_BidStrategy_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/BidStrategy/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_BidStrategy_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_BidStrategyStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/BidStrategyStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_BidStrategyStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_Campaign_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/Campaign/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_Campaign_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_CampaignAssetStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignAssetStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_CampaignAssetStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_CampaignAudienceConversionActionAndDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignAudienceConversionActionAndDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_CampaignAudienceConversionActionAndDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_CampaignAudienceDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignAudienceDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_CampaignAudienceDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_CampaignConversionActionAndAssetStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignConversionActionAndAssetStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_CampaignConversionActionAndAssetStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_CampaignConversionActionAndDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignConversionActionAndDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_CampaignConversionActionAndDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_CampaignCriterion_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignCriterion/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_CampaignCriterion_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_CampaignDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_CampaignDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_CampaignStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/CampaignStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_CampaignStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_CartDataSalesStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/CartDataSalesStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_CartDataSalesStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_Conversion_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/Conversion/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_Conversion_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_ConversionAction_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/ConversionAction/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_ConversionAction_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_GenderConversionActionAndDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/GenderConversionActionAndDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_GenderConversionActionAndDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_GenderDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/GenderDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_GenderDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_Keyword_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/Keyword/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_Keyword_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_KeywordConversionActionAndDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/KeywordConversionActionAndDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_KeywordConversionActionAndDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_KeywordStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/KeywordStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_KeywordStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_LocationConversionActionAndDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/LocationConversionActionAndDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_LocationConversionActionAndDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_LocationDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/LocationDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_LocationDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_NegativeAdGroupCriterion_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/NegativeAdGroupCriterion/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_NegativeAdGroupCriterion_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_NegativeAdGroupKeyword_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/NegativeAdGroupKeyword/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_NegativeAdGroupKeyword_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_NegativeCampaignCriterion_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/NegativeCampaignCriterion/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_NegativeCampaignCriterion_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_NegativeCampaignKeyword_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/NegativeCampaignKeyword/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_NegativeCampaignKeyword_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_ProductAdvertised_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/ProductAdvertised/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_ProductAdvertised_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_ProductAdvertisedConversionActionAndDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/ProductAdvertisedConversionActionAndDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_ProductAdvertisedConversionActionAndDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_ProductAdvertisedDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/ProductAdvertisedDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_ProductAdvertisedDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_ProductGroup_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/ProductGroup/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_ProductGroup_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_ProductGroupStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/ProductGroupStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_ProductGroupStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_Visit_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/Visit/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_Visit_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_WebpageConversionActionAndDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri
    = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/WebpageConversionActionAndDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_WebpageConversionActionAndDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

IF EXISTS (
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES_SUMMARY__`
  WHERE table_id = 'p_sa_WebpageDeviceStats_{CUSTOMER_ID}'
) THEN
  EXPORT DATA OPTIONS (
    uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/WebpageDeviceStats/*.parquet',
    format = 'PARQUET',
    overwrite = TRUE
  ) AS (
    SELECT
      *,
      CURRENT_DATE() AS _LATEST_DATE,
      DATE(_PARTITIONTIME) AS _DATA_DATE
    FROM
      `{PROJECT_ID}.{DATASET_ID}.p_sa_WebpageDeviceStats_{CUSTOMER_ID}`
    WHERE
      DATE(_PARTITIONTIME) = DATE('{RUN_DATE}')
  );
END IF;

EXPORT DATA OPTIONS (
  uri = '{EXPORT_BUCKET_URI}/search_ads/{CUSTOMER_ID}/{RUN_DATE}/Done/*.parquet',
  format = 'PARQUET',
  overwrite = TRUE
) AS (
  SELECT
    TRUE AS done,
    CURRENT_DATE() AS _LATEST_DATE,
    DATE('{RUN_DATE}') AS _DATA_DATE
);
