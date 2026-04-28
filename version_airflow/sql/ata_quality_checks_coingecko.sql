SELECT
  'SANDBOX_TODAY' AS check_name,
  COUNT(*) AS total_rows,
  COUNTIF(coin_id IS NULL) AS null_coin_id,
  COUNTIF(symbol IS NULL) AS null_symbol,
  COUNTIF(current_price IS NULL) AS null_current_price,
  COUNTIF(market_cap_rank IS NULL) AS null_market_cap_rank,
  COUNT(*) - COUNT(DISTINCT CONCAT(coin_id, '-', symbol, '-', source)) AS duplicated_rows
FROM `{project_id}.{sandbox_dataset}.{sandbox_table}`
WHERE ingestion_date = CURRENT_DATE()

UNION ALL

SELECT
  'INTEGRATION_TODAY' AS check_name,
  COUNT(*) AS total_rows,
  COUNTIF(coin_id IS NULL) AS null_coin_id,
  COUNTIF(symbol IS NULL) AS null_symbol,
  COUNTIF(current_price IS NULL) AS null_current_price,
  COUNTIF(market_cap_rank IS NULL) AS null_market_cap_rank,
  COUNT(*) - COUNT(DISTINCT CONCAT(coin_id, '-', symbol, '-', source, '-', CAST(snapshot_date AS STRING))) AS duplicated_rows
FROM `{project_id}.{integration_dataset}.{integration_table}`
WHERE snapshot_date = CURRENT_DATE();