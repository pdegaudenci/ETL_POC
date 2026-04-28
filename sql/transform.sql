MERGE `{project_id}.INTEGRATION.integration_prueba_tecnica` T
USING (
  SELECT
    TIMESTAMP(datetime) AS datetime,
    SAFE_CAST(temperature_2m AS FLOAT64) AS temperature_2m,
    CURRENT_DATE() AS execution_date,
    'open_meteo' AS source
  FROM (
    SELECT
      datetime,
      temperature_2m,
      source,
      ROW_NUMBER() OVER (
        PARTITION BY datetime
        ORDER BY datetime
      ) AS rn
    FROM `{project_id}.SANDBOX_PRUEBA_METEO.open_meteo_hourly`
    WHERE datetime IS NOT NULL
  )
  WHERE rn = 1
) S
ON T.datetime = S.datetime

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