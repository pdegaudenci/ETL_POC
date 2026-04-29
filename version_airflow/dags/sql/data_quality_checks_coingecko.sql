-- ============================================================
-- DATA QUALITY CHECKS - COINGECKO BIGQUERY PIPELINE
-- ============================================================
-- Objetivo:
-- Validar calidad de datos en SANDBOX e INTEGRATION:
-- - Conteo de registros
-- - Nulos en campos clave
-- - Duplicados por clave de negocio
-- - Rangos inválidos
-- - Reconciliación entre capas
-- ============================================================

-- 1. Resumen de calidad SANDBOX e INTEGRATION para el día actual
SELECT
  'SANDBOX_TODAY' AS check_name,
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
  COUNT(*) - COUNT(DISTINCT CONCAT(coin_id, '-', symbol, '-', source, '-', CAST(ingestion_date AS STRING))) AS duplicated_rows
FROM `{project_id}.{sandbox_dataset}.{sandbox_table}`
WHERE ingestion_date = CURRENT_DATE()

UNION ALL

SELECT
  'INTEGRATION_TODAY' AS check_name,
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
  COUNT(*) - COUNT(DISTINCT CONCAT(coin_id, '-', symbol, '-', source, '-', CAST(snapshot_date AS STRING))) AS duplicated_rows
FROM `{project_id}.{integration_dataset}.{integration_table}`
WHERE snapshot_date = CURRENT_DATE();


-- 2. Duplicados en SANDBOX por clave de negocio
SELECT
  coin_id,
  symbol,
  source,
  ingestion_date,
  COUNT(*) AS duplicated_count
FROM `{project_id}.{sandbox_dataset}.{sandbox_table}`
WHERE ingestion_date = CURRENT_DATE()
GROUP BY coin_id, symbol, source, ingestion_date
HAVING COUNT(*) > 1
ORDER BY duplicated_count DESC;


-- 3. Duplicados en INTEGRATION por clave de negocio
SELECT
  coin_id,
  symbol,
  source,
  snapshot_date,
  COUNT(*) AS duplicated_count
FROM `{project_id}.{integration_dataset}.{integration_table}`
WHERE snapshot_date = CURRENT_DATE()
GROUP BY coin_id, symbol, source, snapshot_date
HAVING COUNT(*) > 1
ORDER BY duplicated_count DESC;


-- 4. Validación de rangos inválidos en INTEGRATION
SELECT
  'INVALID_RANGES_INTEGRATION' AS check_name,
  COUNT(*) AS total_rows,
  COUNTIF(current_price <= 0) AS invalid_current_price,
  COUNTIF(market_cap < 0) AS invalid_market_cap,
  COUNTIF(market_cap_rank <= 0) AS invalid_market_cap_rank,
  COUNTIF(total_volume < 0) AS invalid_total_volume,
  MIN(current_price) AS min_current_price,
  MAX(current_price) AS max_current_price,
  AVG(current_price) AS avg_current_price
FROM `{project_id}.{integration_dataset}.{integration_table}`
WHERE snapshot_date = CURRENT_DATE();


-- 5. Reconciliación SANDBOX vs INTEGRATION
SELECT
  'RECONCILIATION_TODAY' AS check_name,
  (
    SELECT COUNT(DISTINCT CONCAT(coin_id, '-', symbol, '-', source, '-', CAST(ingestion_date AS STRING)))
    FROM `{project_id}.{sandbox_dataset}.{sandbox_table}`
    WHERE ingestion_date = CURRENT_DATE()
  ) AS sandbox_distinct_rows,
  (
    SELECT COUNT(DISTINCT CONCAT(coin_id, '-', symbol, '-', source, '-', CAST(snapshot_date AS STRING)))
    FROM `{project_id}.{integration_dataset}.{integration_table}`
    WHERE snapshot_date = CURRENT_DATE()
  ) AS integration_distinct_rows;


-- 6. Top 10 monedas por market cap
SELECT
  market_cap_rank,
  coin_id,
  symbol,
  name,
  current_price,
  market_cap,
  total_volume,
  price_change_percentage_24h,
  snapshot_date,
  source
FROM `{project_id}.{integration_dataset}.{integration_table}`
WHERE snapshot_date = CURRENT_DATE()
ORDER BY market_cap_rank ASC
LIMIT 10;