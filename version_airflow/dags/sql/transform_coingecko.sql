MERGE `{project_id}.{integration_dataset}.{integration_table}` T
USING (
  SELECT
    coin_id,
    symbol,
    name,
    CURRENT_DATE() AS snapshot_date,
    SAFE_CAST(current_price AS FLOAT64) AS current_price,
    SAFE_CAST(market_cap AS INT64) AS market_cap,
    SAFE_CAST(market_cap_rank AS INT64) AS market_cap_rank,
    SAFE_CAST(total_volume AS FLOAT64) AS total_volume,
    SAFE_CAST(price_change_percentage_24h AS FLOAT64) AS price_change_percentage_24h,
    SAFE_CAST(last_updated AS TIMESTAMP) AS last_updated,
    source,
    vs_currency,
    CURRENT_DATE() AS execution_date,
    ingested_at
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY coin_id, symbol, source, ingestion_date
        ORDER BY ingested_at DESC
      ) AS rn
    FROM `{project_id}.{sandbox_dataset}.{sandbox_table}`
    WHERE ingestion_date = CURRENT_DATE()
      AND coin_id IS NOT NULL
      AND symbol IS NOT NULL
  )
  WHERE rn = 1
) S
ON T.coin_id = S.coin_id
AND T.symbol = S.symbol
AND T.snapshot_date = S.snapshot_date
AND T.source = S.source

WHEN MATCHED THEN
UPDATE SET
  name = S.name,
  current_price = S.current_price,
  market_cap = S.market_cap,
  market_cap_rank = S.market_cap_rank,
  total_volume = S.total_volume,
  price_change_percentage_24h = S.price_change_percentage_24h,
  last_updated = S.last_updated,
  vs_currency = S.vs_currency,
  execution_date = S.execution_date,
  ingested_at = S.ingested_at

WHEN NOT MATCHED THEN
INSERT (
  coin_id,
  symbol,
  name,
  snapshot_date,
  current_price,
  market_cap,
  market_cap_rank,
  total_volume,
  price_change_percentage_24h,
  last_updated,
  source,
  vs_currency,
  execution_date,
  ingested_at
)
VALUES (
  S.coin_id,
  S.symbol,
  S.name,
  S.snapshot_date,
  S.current_price,
  S.market_cap,
  S.market_cap_rank,
  S.total_volume,
  S.price_change_percentage_24h,
  S.last_updated,
  S.source,
  S.vs_currency,
  S.execution_date,
  S.ingested_at
);