MERGE `{project_id}.INTEGRATION.integration_prueba_tecnica` T
USING (
  SELECT
    coin_id,
    symbol,
    name,
    SAFE_CAST(current_price AS FLOAT64) AS current_price,
    SAFE_CAST(market_cap AS INT64) AS market_cap,
    SAFE_CAST(market_cap_rank AS INT64) AS market_cap_rank,
    SAFE_CAST(total_volume AS FLOAT64) AS total_volume,
    SAFE_CAST(price_change_percentage_24h AS FLOAT64) AS price_change_percentage_24h,
    TIMESTAMP(last_updated) AS last_updated,
    CURRENT_DATE() AS execution_date,
    source
  FROM (
    SELECT
      coin_id,
      symbol,
      name,
      current_price,
      market_cap,
      market_cap_rank,
      total_volume,
      price_change_percentage_24h,
      last_updated,
      source,
      ingested_at,
      ROW_NUMBER() OVER (
        PARTITION BY coin_id, source
        ORDER BY ingested_at DESC
      ) AS rn
    FROM `{project_id}.SANDBOX_CRYPTO.coingecko_markets`
    WHERE ingestion_date = CURRENT_DATE()
      AND coin_id IS NOT NULL
  )
  WHERE rn = 1
) S
ON T.coin_id = S.coin_id
AND T.source = S.source

WHEN MATCHED THEN
UPDATE SET
  symbol = S.symbol,
  name = S.name,
  current_price = S.current_price,
  market_cap = S.market_cap,
  market_cap_rank = S.market_cap_rank,
  total_volume = S.total_volume,
  price_change_percentage_24h = S.price_change_percentage_24h,
  last_updated = S.last_updated,
  execution_date = S.execution_date,
  source = S.source

WHEN NOT MATCHED THEN
INSERT (
  coin_id,
  symbol,
  name,
  current_price,
  market_cap,
  market_cap_rank,
  total_volume,
  price_change_percentage_24h,
  last_updated,
  execution_date,
  source
)
VALUES (
  S.coin_id,
  S.symbol,
  S.name,
  S.current_price,
  S.market_cap,
  S.market_cap_rank,
  S.total_volume,
  S.price_change_percentage_24h,
  S.last_updated,
  S.execution_date,
  S.source
);