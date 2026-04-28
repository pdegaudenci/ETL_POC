# Prueba Técnica – Python + GCP + BigQuery

## Descripción

Pipeline de ingesta y transformación de datos con **Python**, **Google Cloud Platform** y **BigQuery**.

El proceso:

1. Descarga 100 registros desde la API pública **Open-Meteo**.
2. Carga los datos en `SANDBOX_PRUEBA_METEO.open_meteo_hourly`.
3. Ejecuta `sql/transform.sql`.
4. Inserta el resultado en `INTEGRATION.integration_prueba_tecnica`.
5. Usa `MERGE` para evitar duplicados.

---

## Arquitectura

Open-Meteo API  
↓  
Python ApiDownloader  
↓  
BigQuery SANDBOX_PRUEBA_METEO.open_meteo_hourly  
↓  
sql/transform.sql  
↓  
BigQuery INTEGRATION.integration_prueba_tecnica

---

## Estructura del proyecto

EJERCICIO/  
├── api/  
│ ├── api_downloader.py  
│ ├── bq_loader.py  
│ └── main.py  
├── sql/  
│ ├── transform.sql  
│ └── data_quality_checks.sql  
├── tests/  
├── config.py  
├── requirements.txt  
├── .env.example  
├── .gitignore  
└── README.md

---

## Prerrequisitos

- Python 3.10+
- Proyecto en Google Cloud
- BigQuery API habilitada
- Service Account con roles:
  - BigQuery Data Editor
  - BigQuery Job User
  - opcional: BigQuery Admin

Guardar JSON en:

credentials/service_account.json

---

## Variables de entorno

Crear `.env`

PROJECT_ID=tu-project-id  
BQ_LOCATION=EU

APP_NAME=PRUEBA_METEO

SANDBOX_DATASET=SANDBOX_PRUEBA_METEO  
SANDBOX_TABLE=open_meteo_hourly

INTEGRATION_DATASET=INTEGRATION  
INTEGRATION_TABLE=integration_prueba_tecnica

API_URL=https://api.open-meteo.com/v1/forecast?latitude=40.4&longitude=-3.7&hourly=temperature_2m&forecast_days=5  
API_LIMIT=100

WRITE_DISPOSITION=WRITE_TRUNCATE  
GOOGLE_APPLICATION_CREDENTIALS=credentials/service_account.json

---

## Instalación

pip install -r requirements.txt

---

## Ejecución (desde n raiz de proyecto)

python -m etl.main

---

## Resultado esperado

SANDBOX_PRUEBA_METEO  
└── open_meteo_hourly

INTEGRATION  
└── integration_prueba_tecnica

---

## Diseño BigQuery

### SANDBOX

Campos extra:

- ingestion_date
- ingested_at

Optimización:

- particionada por `ingestion_date`
- clusterizada por `source`

### INTEGRATION

Optimización:

- particionada por `datetime`
- clusterizada por `source`

---

## Transformación SQL

Archivo:

sql/transform.sql

La query:

- filtra partición actual
- elimina duplicados con `ROW_NUMBER()`
- usa clave `datetime + source`
- hace UPSERT con `MERGE`

---

## Calidad de datos

Archivo:

sql/data_quality_checks.sql

Incluye:

- conteo registros
- nulos
- duplicados
- rangos inválidos
- reconciliación tablas

---

## Tests

python -m pytest

Test real API:

python -m pytest tests/integration/test_real_api.py -s

---

## Evidencia

![Tablas BigQuery](https://github.com/pdegaudenci/ETL_POC/blob/master/docs/tablas.png)

---

## Buenas prácticas

- Separación de responsabilidades
- Variables de entorno
- Service Account
- Creación automática de tablas
- MERGE idempotente
- Particionado y clustering
- Data quality checks
- Tests unitarios e integración (implementado pàrciialmente)

---

## Tecnologías

Python, Requests, BigQuery, SQL, Pytest, python-dotenv.