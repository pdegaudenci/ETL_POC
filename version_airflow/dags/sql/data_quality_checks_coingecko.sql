-- ============================================================
-- DATA QUALITY CHECKS - COINGECKO BIGQUERY PIPELINE
-- Persiste resultados en {integration_dataset}.data_quality_results
-- ============================================================

CREATE TABLE IF NOT EXISTS `{project_id}.{integration_dataset}.data_quality_results`
(
  execution_ts TIMESTAMP,
  execution_date DATE,
  source STRING,
  layer STRING,
  check_name STRING,
  total_rows INT64,
  null_coin_id INT64,
  null_symbol INT64,
  null_name INT64,
  null_current_price INT64,
  null_market_cap INT64,
  null_market_cap_rank INT64,
  null_source INT64,
  null_vs_currency INT64,
  null_partition_date INT64,
  null_ingested_at INT64,
  duplicated_rows INT64,
  invalid_current_price INT64,
  invalid_market_cap INT64,
  invalid_market_cap_rank INT64,
  invalid_total_volume INT64,
  sandbox_distinct_rows INT64,
  integration_distinct_rows INT64,
  status STRING
)
PARTITION BY execution_date
CLUSTER BY source, layer, check_name;


INSERT INTO `{project_id}.{integration_dataset}.data_quality_results`
(
  execution_ts,
  execution_date,
  source,
  layer,
  check_name,
  total_rows,
  null_coin_id,
  null_symbol,
  null_name,
  null_current_price,
  null_market_cap,
  null_market_cap_rank,
  null_source,
  null_vs_currency,
  null_partition_date,
  null_ingested_at,
  duplicated_rows,
  invalid_current_price,
  invalid_market_cap,
  invalid_market_cap_rank,
  invalid_total_volume,
  sandbox_distinct_rows,
  integration_distinct_rows,
  status
)

-- 1. SANDBOX QUALITY
SELECT
  CURRENT_TIMESTAMP() AS execution_ts,
  CURRENT_DATE() AS execution_date,
  'coingecko' AS source,
  'SANDBOX' AS layer,
  'SANDBOX_TODAY_QUALITY' AS check_name,
  COUNT(*) AS total_rows,
  COUNTIF(coin_id IS NULL) AS null_coin_id,
  COUNTIF(symbol IS NULL) AS null_symbol,
  COUNTIF(name IS NULL) AS null_name,
  COUNTIF(current_price IS NULL) AS null_current_price,
  COUNTIF(market_cap IS NULL) AS null_market_cap,
  COUNTIF(market_cap_rank IS NULL) AS null_market_cap_rank,
  COUNTIF(source IS NULL) AS null_source,
  COUNTIF(vs_currency IS NULL) AS null_vs_currency,
  COUNTIF(ingestion_date IS NULL) AS null_partition_date,
  COUNTIF(ingested_at IS NULL) AS null_ingested_at,
  COUNT(*) - COUNT(DISTINCT CONCAT(coin_id, '-', symbol, '-', source, '-', CAST(ingestion_date AS STRING))) AS duplicated_rows,
  COUNTIF(current_price <= 0) AS invalid_current_price,
  COUNTIF(market_cap < 0) AS invalid_market_cap,
  COUNTIF(market_cap_rank <= 0) AS invalid_market_cap_rank,
  COUNTIF(total_volume < 0) AS invalid_total_volume,
  NULL AS sandbox_distinct_rows,
  NULL AS integration_distinct_rows,
  CASE
    WHEN COUNT(*) = 0 THEN 'FAIL'
    WHEN COUNTIF(coin_id IS NULL) > 0 THEN 'FAIL'
    WHEN COUNTIF(symbol IS NULL) > 0 THEN 'FAIL'
    WHEN COUNTIF(current_price IS NULL) > 0 THEN 'FAIL'
    WHEN COUNTIF(current_price <= 0) > 0 THEN 'FAIL'
    ELSE 'OK'
  END AS status
FROM `{project_id}.{sandbox_dataset}.{sandbox_table}`
WHERE ingestion_date = CURRENT_DATE()

UNION ALL

-- 2. INTEGRATION QUALITY
SELECT
  CURRENT_TIMESTAMP() AS execution_ts,
  CURRENT_DATE() AS execution_date,
  'coingecko' AS source,
  'INTEGRATION' AS layer,
  'INTEGRATION_TODAY_QUALITY' AS check_name,
  COUNT(*) AS total_rows,
  COUNTIF(coin_id IS NULL) AS null_coin_id,
  COUNTIF(symbol IS NULL) AS null_symbol,
  COUNTIF(name IS NULL) AS null_name,
  COUNTIF(current_price IS NULL) AS null_current_price,
  COUNTIF(market_cap IS NULL) AS null_market_cap,
  COUNTIF(market_cap_rank IS NULL) AS null_market_cap_rank,
  COUNTIF(source IS NULL) AS null_source,
  COUNTIF(vs_currency IS NULL) AS null_vs_currency,
  COUNTIF(snapshot_date IS NULL) AS null_partition_date,
  COUNTIF(ingested_at IS NULL) AS null_ingested_at,
  COUNT(*) - COUNT(DISTINCT CONCAT(coin_id, '-', symbol, '-', source, '-', CAST(snapshot_date AS STRING))) AS duplicated_rows,
  COUNTIF(current_price <= 0) AS invalid_current_price,
  COUNTIF(market_cap < 0) AS invalid_market_cap,
  COUNTIF(market_cap_rank <= 0) AS invalid_market_cap_rank,
  COUNTIF(total_volume < 0) AS invalid_total_volume,
  NULL AS sandbox_distinct_rows,
  NULL AS integration_distinct_rows,
  CASE
    WHEN COUNT(*) = 0 THEN 'FAIL'
    WHEN COUNTIF(coin_id IS NULL) > 0 THEN 'FAIL'
    WHEN COUNTIF(symbol IS NULL) > 0 THEN 'FAIL'
    WHEN COUNTIF(current_price IS NULL) > 0 THEN 'FAIL'
    WHEN COUNTIF(current_price <= 0) > 0 THEN 'FAIL'
    WHEN COUNT(*) - COUNT(DISTINCT CONCAT(coin_id, '-', symbol, '-', source, '-', CAST(snapshot_date AS STRING))) > 0 THEN 'FAIL'
    ELSE 'OK'
  END AS status
FROM `{project_id}.{integration_dataset}.{integration_table}`
WHERE snapshot_date = CURRENT_DATE()

UNION ALL

-- 3. RECONCILIATION SANDBOX VS INTEGRATION
SELECT
  CURRENT_TIMESTAMP() AS execution_ts,
  CURRENT_DATE() AS execution_date,
  'coingecko' AS source,
  'RECONCILIATION' AS layer,
  'SANDBOX_VS_INTEGRATION_TODAY' AS check_name,
  NULL AS total_rows,
  NULL AS null_coin_id,
  NULL AS null_symbol,
  NULL AS null_name,
  NULL AS null_current_price,
  NULL AS null_market_cap,
  NULL AS null_market_cap_rank,
  NULL AS null_source,
  NULL AS null_vs_currency,
  NULL AS null_partition_date,
  NULL AS null_ingested_at,
  NULL AS duplicated_rows,
  NULL AS invalid_current_price,
  NULL AS invalid_market_cap,
  NULL AS invalid_market_cap_rank,
  NULL AS invalid_total_volume,
  sandbox_count AS sandbox_distinct_rows,
  integration_count AS integration_distinct_rows,
  CASE
    WHEN sandbox_count = integration_count THEN 'OK'
    ELSE 'FAIL'
  END AS status
FROM (
  SELECT
    (
      SELECT COUNT(DISTINCT CONCAT(coin_id, '-', symbol, '-', source, '-', CAST(ingestion_date AS STRING)))
      FROM `{project_id}.{sandbox_dataset}.{sandbox_table}`
      WHERE ingestion_date = CURRENT_DATE()
    ) AS sandbox_count,
    (
      SELECT COUNT(DISTINCT CONCAT(coin_id, '-', symbol, '-', source, '-', CAST(snapshot_date AS STRING)))
      FROM `{project_id}.{integration_dataset}.{integration_table}`
      WHERE snapshot_date = CURRENT_DATE()
    ) AS integration_count
);