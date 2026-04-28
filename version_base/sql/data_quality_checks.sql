-- ============================================================
-- DATA QUALITY CHECKS
-- ============================================================

-- 1. Calidad general SANDBOX por partición del día
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(datetime IS NULL) AS null_datetime,
  COUNTIF(temperature_2m IS NULL) AS null_temperature,
  COUNTIF(source IS NULL) AS null_source,
  COUNTIF(ingestion_date IS NULL) AS null_ingestion_date,
  COUNTIF(ingested_at IS NULL) AS null_ingested_at,
  COUNT(DISTINCT CONCAT(datetime, source)) AS distinct_business_key,
  COUNT(*) - COUNT(DISTINCT CONCAT(datetime, source)) AS duplicated_rows
FROM `{project_id}.SANDBOX_PRUEBA_METEO.open_meteo_hourly`
WHERE ingestion_date = CURRENT_DATE();


-- 2. Duplicados en SANDBOX por clave natural
SELECT
  datetime,
  source,
  COUNT(*) AS duplicated_count
FROM `{project_id}.SANDBOX_PRUEBA_METEO.open_meteo_hourly`
WHERE ingestion_date = CURRENT_DATE()
GROUP BY datetime, source
HAVING COUNT(*) > 1
ORDER BY duplicated_count DESC;


-- 3. Calidad general INTEGRATION
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(datetime IS NULL) AS null_datetime,
  COUNTIF(temperature_2m IS NULL) AS null_temperature,
  COUNTIF(execution_date IS NULL) AS null_execution_date,
  COUNTIF(source IS NULL) AS null_source,
  COUNT(DISTINCT CONCAT(CAST(datetime AS STRING), source)) AS distinct_business_key,
  COUNT(*) - COUNT(DISTINCT CONCAT(CAST(datetime AS STRING), source)) AS duplicated_rows
FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`;


-- 4. Duplicados en INTEGRATION por clave natural
SELECT
  datetime,
  source,
  COUNT(*) AS duplicated_count
FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`
GROUP BY datetime, source
HAVING COUNT(*) > 1
ORDER BY duplicated_count DESC;


-- 5. Validación de rango de temperatura
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(temperature_2m < -50 OR temperature_2m > 60) AS invalid_temperature_count,
  MIN(temperature_2m) AS min_temperature,
  MAX(temperature_2m) AS max_temperature,
  AVG(temperature_2m) AS avg_temperature
FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`;


-- 6. Registros con temperatura fuera de rango
SELECT
  datetime,
  temperature_2m,
  source,
  execution_date
FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`
WHERE temperature_2m < -50
   OR temperature_2m > 60
ORDER BY datetime;


-- 7. Reconciliación SANDBOX vs INTEGRATION para la partición del día
SELECT
  (
    SELECT COUNT(DISTINCT CONCAT(datetime, source))
    FROM `{project_id}.SANDBOX_PRUEBA_METEO.open_meteo_hourly`
    WHERE ingestion_date = CURRENT_DATE()
  ) AS sandbox_distinct_rows_today,
  (
    SELECT COUNT(DISTINCT CONCAT(CAST(datetime AS STRING), source))
    FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`
    WHERE DATE(datetime) >= CURRENT_DATE()
  ) AS integration_distinct_rows_today;


-- 8. Registros de SANDBOX del día que no llegaron a INTEGRATION
SELECT
  s.datetime,
  s.source,
  s.temperature_2m,
  s.ingestion_date,
  s.ingested_at
FROM `{project_id}.SANDBOX_PRUEBA_METEO.open_meteo_hourly` s
LEFT JOIN `{project_id}.INTEGRATION.integration_prueba_tecnica` i
  ON TIMESTAMP(s.datetime) = i.datetime
 AND s.source = i.source
WHERE s.ingestion_date = CURRENT_DATE()
  AND i.datetime IS NULL
ORDER BY s.datetime;


-- 9. Resumen final de calidad por tabla
SELECT
  'SANDBOX' AS table_name,
  COUNT(*) AS total_rows,
  COUNTIF(datetime IS NULL) AS null_datetime,
  COUNTIF(temperature_2m IS NULL) AS null_temperature,
  COUNT(*) - COUNT(DISTINCT CONCAT(datetime, source)) AS duplicated_rows
FROM `{project_id}.SANDBOX_PRUEBA_METEO.open_meteo_hourly`
WHERE ingestion_date = CURRENT_DATE()

UNION ALL

SELECT
  'INTEGRATION' AS table_name,
  COUNT(*) AS total_rows,
  COUNTIF(datetime IS NULL) AS null_datetime,
  COUNTIF(temperature_2m IS NULL) AS null_temperature,
  COUNT(*) - COUNT(DISTINCT CONCAT(CAST(datetime AS STRING), source)) AS duplicated_rows
FROM `{project_id}.INTEGRATION.integration_prueba_tecnica`;