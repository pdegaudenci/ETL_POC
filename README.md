# Prueba Técnica – Python + GCP + BigQuery + Cloud Composer

## Descripción

Proyecto de ingeniería de datos con dos implementaciones:

### 1. Versión Base (Python ETL)

Pipeline de ingesta y transformación con **Python**, **GCP** y **BigQuery**.

Proceso:

1. Descarga 100 registros desde la API pública **Open-Meteo**
2. Carga datos en tabla SANDBOX
3. Ejecuta transformación SQL
4. Inserta resultado en tabla INTEGRATION
5. Usa `MERGE` para evitar duplicados

### 2. Versión Profesional (Cloud Composer)

Pipeline orquestado con **Apache Airflow**, **Cloud Composer**, **BigQuery** y **CoinGecko**.

Proceso:

1. Descarga mercado crypto desde CoinGecko
2. Crea datasets y tablas automáticamente
3. Carga datos crudos en SANDBOX
4. Ejecuta transformación SQL idempotente
5. Ejecuta validaciones de calidad
6. Pipeline cloud gestionado y escalable

---

# Arquitectura

## Versión Base

    Open-Meteo API
    ↓
    Python ApiDownloader
    ↓
    BigQuery SANDBOX
    ↓
    transform.sql
    ↓
    BigQuery INTEGRATION

## Versión Cloud Composer

    CoinGecko API
    ↓
    Cloud Composer (Airflow DAG)
    ↓
    BigQuery SANDBOX
    ↓
    transform_coingecko.sql
    ↓
    BigQuery INTEGRATION
    ↓
    Data Quality Checks

---

# Estructura del proyecto

    EJERCICIO/
    ├── version_base/
    │   ├── etl/
    │   ├── sql/
    │   ├── tests/
    │   ├── config.py
    │   ├── requirements.txt
    │   ├── setup_base.ps1
    │   ├── run_base.ps1
    │   └── .env.example
    │
    ├── version_airflow/
    │   ├── dags/
    │   │   ├── coingecko_market_pipeline_dag.py
    │   │   ├── utils/
    │   │   │   ├── __init__.py
    │   │   │   ├── config.py
    │   │   │   ├── coingecko_api.py
    │   │   │   └── schemas.py
    │   │   └── sql/
    │   │       ├── transform_coingecko.sql
    │   │       └── data_quality_checks_coingecko.sql
    │   ├── requirements.txt
    │   └── .env.example
    │
    ├── .gitignore
    └── README.md

---

# Prerrequisitos

    Python 3.10+
    Proyecto en Google Cloud
    BigQuery API habilitada
    Cloud Composer habilitado
    Cuenta de servicio IAM

---

# Service Account

Crear:

    composer-etl-sa@PROJECT_ID.iam.gserviceaccount.com

## Roles recomendados

    Composer Worker
    BigQuery Data Editor
    BigQuery Job User
    Storage Object Viewer
    Storage Object Creator

## Qué permite cada rol

**Composer Worker**

- Ejecutar scheduler
- Ejecutar workers
- Ejecutar DAGs

**BigQuery Data Editor**

- Crear tablas
- Insertar datos
- Actualizar tablas
- MERGE / UPDATE / INSERT

**BigQuery Job User**

- Ejecutar queries
- Lanzar jobs SQL

**Storage Object Viewer**

- Leer DAGs
- Leer SQL
- Leer objetos bucket

**Storage Object Creator**

- Subir archivos
- Crear objetos bucket

## Opción rápida demo

    Composer Worker
    BigQuery Admin
    Storage Admin

---

# Variables de entorno (Versión Base)

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

# Variables Airflow (Versión Composer)

En Airflow UI → Admin → Variables

    PROJECT_ID = tu-project-id
    BQ_LOCATION = EU

    SANDBOX_DATASET = SANDBOX_PRUEBA_CRYPTO
    SANDBOX_TABLE = coingecko_markets_raw

    INTEGRATION_DATASET = INTEGRATION
    INTEGRATION_TABLE = integration_coingecko_markets

    API_URL = https://api.coingecko.com/api/v3/coins/markets
    API_VS_CURRENCY = usd
    API_ORDER = market_cap_desc
    API_LIMIT = 100
    API_PAGE = 1
    API_SPARKLINE = false

    SOURCE_NAME = coingecko
    WRITE_DISPOSITION = WRITE_TRUNCATE

---

# Instalación

## Versión Base

Instalar dependencias:

    pip install -r version_base/requirements.txt

## Versión Composer

En Composer → PyPI packages:

    requests==2.32.3
    google-cloud-bigquery==3.25.0

---

# Ejecución

## Versión Base – Opción 1 (manual)

Desde la carpeta `version_base`:

    python -m etl.main

## Versión Base – Opción 2 (PowerShell con scripts)

Entrar en:

    cd version_base

Permitir scripts:

    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

Preparar entorno:

    .\setup_base.ps1

Ejecutar pipeline:

    .\run_base.ps1

## Qué hacen los scripts

### setup_base.ps1

- Crea entorno virtual
- Activa entorno virtual
- Instala requirements.txt

### run_base.ps1

- Activa entorno virtual
- Configura PYTHONPATH
- Ejecuta:

    python -m etl.main

## Versión Composer

1. Crear entorno Cloud Composer
2. Subir DAGs
3. Configurar Variables
4. Ejecutar DAG desde Airflow UI

Ruta:

    DAGs → coingecko_market_pipeline
    Unpause
    Trigger DAG

---

# Despliegue Cloud Composer

## Crear entorno

Ir a:

    Cloud Composer → Create Environment

Configurar:

    Environment name: composer-coingecko-dev
    Region: europe-west1
    Composer version: estable
    Airflow version: estable
    Service Account: composer-etl-sa
    Network: Public IP

## Subir archivos al bucket DAGs

Subir contenido interno de:

    version_airflow/dags/

Estructura final en Composer:

    dags/
    ├── coingecko_market_pipeline_dag.py
    ├── utils/
    └── sql/

---

# Orden de ejecución DAG

    create_sandbox_dataset
    create_integration_dataset
    create_sandbox_table
    create_integration_table
    extract_coingecko_data
    load_to_sandbox
    check_sandbox_has_rows
    transform_to_integration
    check_integration_has_rows
    check_no_duplicate_business_key
    run_data_quality_summary

---

# Resultado esperado

## Versión Base

    SANDBOX_PRUEBA_METEO.open_meteo_hourly
    INTEGRATION.integration_prueba_tecnica

## Versión Composer

    SANDBOX_PRUEBA_CRYPTO.coingecko_markets_raw
    INTEGRATION.integration_coingecko_markets

---

# Diseño BigQuery

## SANDBOX

Incluye campos técnicos:

    ingestion_date
    ingested_at

Optimización:

    particionado por fecha
    clustering por source

## INTEGRATION

Optimización:

    particionado por snapshot_date / datetime
    clustering por source

---

# Transformación SQL

## Versión Base

Archivo:

    sql/transform.sql

Lógica:

- Filtra partición actual
- Elimina duplicados con ROW_NUMBER()
- Usa clave datetime + source
- UPSERT con MERGE

## Versión Composer

Archivo:

    dags/sql/transform_coingecko.sql

Lógica:

- Filtra ejecución actual
- Deduplicación
- Clave coin_id + symbol + snapshot_date + source
- MERGE idempotente

---

# Calidad de datos

Archivos:

    sql/data_quality_checks.sql
    dags/sql/data_quality_checks_coingecko.sql

Incluye:

- Conteo registros
- Nulos
- Duplicados
- Reconciliación
- Validaciones finales

---

# Tests

    python -m pytest

Test API real:

    python -m pytest tests/integration/test_real_api.py -s

---

# Evidencia

![Tablas BigQuery](https://github.com/pdegaudenci/ETL_POC/blob/master/docs/tablas.png)

---

# Buenas prácticas aplicadas

    Separación de responsabilidades
    Configuración desacoplada
    Variables de entorno
    Airflow Variables
    Service Account dedicada
    Creación automática de tablas
    MERGE idempotente
    Particionado y clustering
    SQL desacoplado
    Data Quality Checks
    Tests
    Arquitectura modular
    Orquestación cloud

---

# Tecnologías

    Python
    Requests
    BigQuery
    SQL
    Pytest
    python-dotenv
    Apache Airflow
    Cloud Composer
    Google Cloud IAM
    Cloud Storage

---
 Autor : Sebastian Degaudenci