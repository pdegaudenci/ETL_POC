MERGE `{project_id}.{integration_dataset}.{integration_table}` T
USING (
  SELECT
    coin_id,
    symbol,
    name,
    CURRENT_DATE() AS snapshot_date,
    SAFE_CAST(current_price AS FLOAT64) AS current_price,
    SAFE_CAST(market_cap AS FLOAT64) AS market_cap,
    SAFE_CAST(market_cap_rank AS INT64) AS market_cap_rank,
    SAFE_CAST(total_volume AS FLOAT64) AS total_volume,
    SAFE_CAST(high_24h AS FLOAT64) AS high_24h,
    SAFE_CAST(low_24h AS FLOAT64) AS low_24h,
    SAFE_CAST(price_change_24h AS FLOAT64) AS price_change_24h,
    SAFE_CAST(price_change_percentage_24h AS FLOAT64) AS price_change_percentage_24h,
    SAFE_CAST(circulating_supply AS FLOAT64) AS circulating_supply,
    SAFE_CAST(total_supply AS FLOAT64) AS total_supply,
    SAFE_CAST(max_supply AS FLOAT64) AS max_supply,
    SAFE_CAST(ath AS FLOAT64) AS ath,
    SAFE_CAST(ath_change_percentage AS FLOAT64) AS ath_change_percentage,
    SAFE_CAST(ath_date AS TIMESTAMP) AS ath_date,
    SAFE_CAST(atl AS FLOAT64) AS atl,
    SAFE_CAST(atl_change_percentage AS FLOAT64) AS atl_change_percentage,
    SAFE_CAST(atl_date AS TIMESTAMP) AS atl_date,
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
  high_24h = S.high_24h,
  low_24h = S.low_24h,
  price_change_24h = S.price_change_24h,
  price_change_percentage_24h = S.price_change_percentage_24h,
  circulating_supply = S.circulating_supply,
  total_supply = S.total_supply,
  max_supply = S.max_supply,
  ath = S.ath,
  ath_change_percentage = S.ath_change_percentage,
  ath_date = S.ath_date,
  atl = S.atl,
  atl_change_percentage = S.atl_change_percentage,
  atl_date = S.atl_date,
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
  high_24h,
  low_24h,
  price_change_24h,
  price_change_percentage_24h,
  circulating_supply,
  total_supply,
  max_supply,
  ath,
  ath_change_percentage,
  ath_date,
  atl,
  atl_change_percentage,
  atl_date,
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
  S.high_24h,
  S.low_24h,
  S.price_change_24h,
  S.price_change_percentage_24h,
  S.circulating_supply,
  S.total_supply,
  S.max_supply,
  S.ath,
  S.ath_change_percentage,
  S.ath_date,
  S.atl,
  S.atl_change_percentage,
  S.atl_date,
  S.last_updated,
  S.source,
  S.vs_currency,
  S.execution_date,
  S.ingested_at
);