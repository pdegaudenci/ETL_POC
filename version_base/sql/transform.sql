MERGE `{project_id}.INTEGRATION.integration_prueba_tecnica` T
USING (
  SELECT
    TIMESTAMP(datetime) AS datetime,
    SAFE_CAST(temperature_2m AS FLOAT64) AS temperature_2m,
    CURRENT_DATE() AS execution_date,
    source
  FROM (
    SELECT
      datetime,
      temperature_2m,
      source,
      ingested_at,
      ROW_NUMBER() OVER (
        PARTITION BY datetime, source
        ORDER BY ingested_at DESC
      ) AS rn
    FROM `{project_id}.SANDBOX_PRUEBA_METEO.open_meteo_hourly`
    WHERE datetime IS NOT NULL
      AND ingestion_date = CURRENT_DATE()
  )
  WHERE rn = 1
) S
ON T.datetime = S.datetime
AND T.source = S.source

WHEN MATCHED THEN
UPDATE SET
  temperature_2m = S.temperature_2m,
  execution_date = S.execution_date,
  source = S.source

WHEN NOT MATCHED THEN
INSERT (
  datetime,
  temperature_2m,
  execution_date,
  source
)
VALUES (
  S.datetime,
  S.temperature_2m,
  S.execution_date,
  S.source
);