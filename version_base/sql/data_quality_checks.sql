-- ============================================================
-- DATA QUALITY CHECKS - COINGECKO BIGQUERY PIPELINE
-- ============================================================

-- 1. Calidad general SANDBOX por partición actual
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(coin_id IS NULL) AS null_coin_id,
  COUNTIF(symbol IS NULL) AS null_symbol,
  COUNTIF(name IS NULL) AS null_name,
  COUNTIF(current_price IS NULL) AS null_current_price,
  COUNTIF(market_cap IS NULL) AS null_market_cap,
  COUNTIF(market_cap_rank IS NULL) AS null_market_cap_rank,
  COUNTIF(source IS NULL) AS null_source,
  COUNTIF(ingestion_date IS NULL) AS null_ingestion_date,
  COUNTIF(ingested_at IS NULL) AS null_ingested_at,
  COUNT(DISTINCT CONCAT(coin_id, source)) AS distinct_business_key,
  COUNT(*) - COUNT(DISTINCT CONCAT(coin_id, source)) AS duplicated_rows
FROM `{project_id}.SANDBOX_CRYPTO.coingecko_markets`
WHERE ingestion_date = CURRENT_DATE();


-- 2. Duplicados SANDBOX por clave natural
SELECT
  coin_id,
  source,
  COUNT(*) AS duplicated_count
FROM `{project_id}.SANDBOX_CRYPTO.coingecko_markets`
WHERE ingestion_date = CURRENT_DATE()
GROUP BY coin_id, source
HAVING COUNT(*) > 1
ORDER BY duplicated_count DESC;


-- 3. Calidad general INTEGRATION
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(coin_id IS NULL) AS null_coin_id,
  COUNTIF(symbol IS NULL) AS null_symbol,
  COUNTIF(name IS NULL) AS null_name,
  COUNTIF(current_price IS NULL) AS null_current_price,
  COUNTIF(market_cap IS NULL) AS null_market_cap,
  COUNTIF(market_cap_rank IS NULL) AS null_market_cap_rank,
  COUNTIF(execution_date IS NULL) AS null_execution_date,
  COUNTIF(source IS NULL) AS null_source,
  COUNT(DISTINCT CONCAT(coin_id, source)) AS distinct_business_key,
  COUNT(*) - COUNT(DISTINCT CONCAT(coin_id, source)) AS duplicated_rows
FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`;


-- 4. Duplicados INTEGRATION por clave natural
SELECT
  coin_id,
  source,
  COUNT(*) AS duplicated_count
FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`
GROUP BY coin_id, source
HAVING COUNT(*) > 1
ORDER BY duplicated_count DESC;


-- 5. Validación de precios inválidos
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(current_price <= 0) AS invalid_price_count,
  MIN(current_price) AS min_price,
  MAX(current_price) AS max_price,
  AVG(current_price) AS avg_price
FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`;


-- 6. Validación de market cap inválido
SELECT
  COUNTIF(market_cap < 0) AS invalid_market_cap_count,
  MIN(market_cap) AS min_market_cap,
  MAX(market_cap) AS max_market_cap
FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`;


-- 7. Validación de ranking inválido
SELECT
  COUNTIF(market_cap_rank <= 0) AS invalid_rank_count,
  MIN(market_cap_rank) AS min_rank,
  MAX(market_cap_rank) AS max_rank
FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`;


-- 8. Top 10 monedas por market cap
SELECT
  market_cap_rank,
  symbol,
  name,
  current_price,
  market_cap
FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`
ORDER BY market_cap_rank
LIMIT 10;


-- 9. Reconciliación SANDBOX vs INTEGRATION
SELECT
  (
    SELECT COUNT(DISTINCT CONCAT(coin_id, source))
    FROM `{project_id}.SANDBOX_CRYPTO.coingecko_markets`
    WHERE ingestion_date = CURRENT_DATE()
  ) AS sandbox_distinct_coins_today,
  (
    SELECT COUNT(DISTINCT CONCAT(coin_id, source))
    FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`
    WHERE execution_date = CURRENT_DATE()
  ) AS integration_distinct_coins_today;


-- 10. Registros de SANDBOX no presentes en INTEGRATION
SELECT
  s.coin_id,
  s.symbol,
  s.name,
  s.current_price,
  s.market_cap,
  s.market_cap_rank
FROM `{project_id}.SANDBOX_CRYPTO.coingecko_markets` s
LEFT JOIN `{project_id}.INTEGRATION.integration_prueba_tecnica` i
  ON s.coin_id = i.coin_id
 AND s.source = i.source
WHERE s.ingestion_date = CURRENT_DATE()
  AND i.coin_id IS NULL
ORDER BY s.market_cap_rank;


-- 11. Resumen final por tabla
SELECT
  'SANDBOX' AS table_name,
  COUNT(*) AS total_rows,
  COUNTIF(coin_id IS NULL) AS null_coin_id,
  COUNT(*) - COUNT(DISTINCT CONCAT(coin_id, source)) AS duplicated_rows
FROM `{project_id}.SANDBOX_CRYPTO.coingecko_markets`
WHERE ingestion_date = CURRENT_DATE()

UNION ALL

SELECT
  'INTEGRATION' AS table_name,
  COUNT(*) AS total_rows,
  COUNTIF(coin_id IS NULL) AS null_coin_id,
  COUNT(*) - COUNT(DISTINCT CONCAT(coin_id, source)) AS duplicated_rows
FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`;