# Prueba Técnica – Python + GCP + BigQuery + Cloud Composer

## Descripción

Proyecto de ingeniería de datos con dos implementaciones complementarias:

### 1. Versión Base (Python ETL)

Pipeline ETL local desarrollado en Python con arquitectura modular:

**Extract → Load → Transform**

Flujo:

1. Descarga 100 registros desde la API pública **CoinGecko**
2. Guarda copia local en carpeta `output/`
3. Crea dataset y tabla SANDBOX automáticamente
4. Carga datos crudos en BigQuery
5. Ejecuta transformación SQL idempotente
6. Publica datos en `INTEGRATION.integration_prueba_tecnica`

### 2. Versión Profesional (Cloud Composer)

Versión cloud orquestada con:

* Apache Airflow
* Cloud Composer
* BigQuery
* CoinGecko API

Flujo:

1. Extracción desde CoinGecko
2. Carga en SANDBOX
3. Transformación con BigQuery Jobs
4. Data Quality Checks
5. Pipeline escalable y gestionado en GCP

---

## Arquitectura

### Versión Base

```text
CoinGecko API
   ↓
Python ETL
   ↓
output/*.json
   ↓
BigQuery SANDBOX_CRYPTO
   ↓
sql/transform.sql
   ↓
BigQuery INTEGRATION
```

### Versión Cloud Composer

```text
CoinGecko API
   ↓
Cloud Composer / Airflow
   ↓
BigQuery SANDBOX
   ↓
BigQuery MERGE
   ↓
INTEGRATION
   ↓
Quality Checks
```

---

## Estructura del proyecto

```text
EJERCICIO/
├── version_base/
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── extract.py
│   │   ├── load.py
│   │   ├── transform.py
│   │   └── main.py
│   ├── sql/
│   │   ├── transform.sql
│   │   └── data_quality_checks.sql
│   ├── output/
│   ├── tests/
│   ├── requirements.txt
│   └── .env.example
│
├── version_airflow/
│   ├── dags/
│   ├── requirements.txt
│   └── .env.example
│
├── docs/
└── README.md
```

---

## Prerrequisitos

* Python 3.10+
* Cuenta en Google Cloud Platform
* BigQuery API habilitada
* Cloud Composer API habilitada
* Cuenta de servicio IAM
* Clave JSON descargada

---

## Configuración GCP

### 1. Crear proyecto

Crear proyecto:

```text
etl-poc-494716
```

### 2. Habilitar APIs

Activar:

* BigQuery API
* Cloud Composer API
* Cloud Storage API
* IAM API
* Kubernetes Engine API

### 3. Crear Service Account

Crear:

```text
etl-bq-sa
```

### 4. Roles recomendados

Asignar:

* BigQuery Data Editor
* BigQuery Job User
* Storage Object Admin

Opcional demo rápida:

* BigQuery Admin

### 5. Descargar clave JSON

Guardar en:

```text
credentials/service_account.json
```

---

## Variables de entorno (.env)

```env
PROJECT_ID=
BQ_LOCATION=EU

APP_NAME=CRYPTO

SANDBOX_DATASET=SANDBOX_CRYPTO
SANDBOX_TABLE=coingecko_markets

INTEGRATION_DATASET=INTEGRATION
INTEGRATION_TABLE=integration_prueba_tecnica

API_URL=https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1
API_LIMIT=100
SOURCE_NAME=coingecko

WRITE_DISPOSITION=WRITE_TRUNCATE

GOOGLE_APPLICATION_CREDENTIALS=credentials/service_account.json
```

---

## Instalación

Desde `version_base/`

```bash
pip install -r requirements.txt
```

---

## Ejecución

Desde carpeta `version_base`

```bash
python -m etl.main
```

---

## Resultado esperado

### Carpeta local

```text
output/
└── coingecko_markets_YYYYMMDD_HHMMSS.json
```

### BigQuery

```text
SANDBOX_CRYPTO.coingecko_markets
INTEGRATION.integration_prueba_tecnica
```

---

## Diseño BigQuery

### SANDBOX

Tabla raw con campos técnicos:

* ingestion_date
* ingested_at

Optimizada con:

* Particionado por `ingestion_date`
* Clustering por `symbol`

### INTEGRATION

Tabla final de negocio optimizada con:

* Particionado por `execution_date`
* Clustering por `symbol`

---

## Transformación SQL

Archivo:

```text
sql/transform.sql
```

Características:

* Lee datos desde SANDBOX
* Filtra partición actual
* Deduplica con `ROW_NUMBER()`
* Usa `MERGE`
* Idempotente
* Crea tabla si no existe desde Python

---

## Data Quality Checks

Archivo:

```text
sql/data_quality_checks.sql
```

Validaciones incluidas:

* Conteo de registros
* Nulos
* Duplicados
* Rangos inválidos
* Reconciliación SANDBOX vs INTEGRATION
* Top market cap
* Resumen final

---

## Tests

Ejecutar todos:

```bash
python -m pytest
```

Ejecutar API real:

```bash
python -m pytest tests/integration/test_real_api.py -s
```

---

## Cloud Composer (versión avanzada)

### Qué demuestra

* Orquestación profesional
* DAGs en Airflow
* Operadores BigQuery
* Variables Airflow
* Reintentos
* Dependencias entre tareas
* Escalabilidad cloud

---

## Despliegue en Cloud Composer (Paso a Paso)

### 1. Crear entorno Composer

Ir a Google Cloud Console:

```text
Cloud Composer → Create Environment
```

Configurar:

```text
Name: composer-etl
Region: europe-west1
Composer Version: Composer 3
Environment Size: Small
Network: default
Service Account: composer-etl-sa
```

Esperar creación (20–30 min aprox).

---

### 2. Instalar dependencias Python

Entrar al entorno Composer creado:

```text
Environment → PyPI packages
```

Agregar:

```text
requests==2.32.3
google-cloud-bigquery==3.25.0
```

Guardar y esperar actualización.

---

### 3. Abrir bucket del entorno

Dentro del entorno Composer:

```text
Open Environment Details → DAGs Folder
```

Se abrirá el bucket Cloud Storage asociado.

---

### 4. Subir archivos del proyecto

Subir contenido interno de:

```text
version_airflow/dags/
```

Debe quedar:

```text
dags/
├── coingecko_market_pipeline_dag.py
├── utils/
│   ├── __init__.py
│   ├── config.py
│   ├── coingecko_api.py
│   └── schemas.py
└── sql/
    ├── transform_coingecko.sql
    └── data_quality_checks_coingecko.sql
```

---

### 5. Configurar Variables de Airflow

Abrir Airflow UI:

```text
Admin → Variables
```

Crear:

```text
PROJECT_ID = 
BQ_LOCATION = EU

SANDBOX_DATASET = SANDBOX_CRYPTO
SANDBOX_TABLE = coingecko_markets

INTEGRATION_DATASET = INTEGRATION
INTEGRATION_TABLE = integration_prueba_tecnica

API_URL = https://api.coingecko.com/api/v3/coins/markets
API_VS_CURRENCY = usd
API_ORDER = market_cap_desc
API_LIMIT = 100
API_PAGE = 1
API_SPARKLINE = false

SOURCE_NAME = coingecko
WRITE_DISPOSITION = WRITE_TRUNCATE
```

---

### 6. Esperar carga del DAG

En 1–5 minutos aparecerá:

```text
coingecko_market_pipeline
```

---

### 7. Activar DAG

En Airflow:

* Buscar DAG
* Activar con toggle (Unpause)

---

### 8. Ejecutar pipeline

Pulsar:

```text
Trigger DAG
```

---

### 9. Orden esperado de ejecución

```text
create_sandbox_dataset
create_integration_dataset
create_sandbox_table
create_integration_table
extract_coingecko_data
load_to_sandbox
check_sandbox_has_rows
transform_to_integration
check_integration_has_rows
check_no_duplicates
run_data_quality_checks
```

---

### 10. Verificar BigQuery

Deben aparecer:

```text
SANDBOX_CRYPTO.coingecko_markets
INTEGRATION.integration_prueba_tecnica
```

---

### 11. Logs y troubleshooting

Revisar:

* Airflow Task Logs
* Cloud Logging
* Composer Logs

---

## Evidencia

![Tablas BigQuery](https://github.com/pdegaudenci/ETL_POC/blob/master/version_base/docs/tablas.png)

---

## Buenas prácticas aplicadas

* Arquitectura modular
* Separación de responsabilidades
* Variables de entorno
* SQL desacoplado
* Idempotencia
* MERGE
* Particionado
* Clustering
* Tests
* Persistencia local output/
* Cloud ready
* Escalable

---

## Tecnologías utilizadas

* Python
* Requests
* Google Cloud
* BigQuery
* SQL
* Pytest
* python-dotenv
* Apache Airflow
* Cloud Composer

---

## Autor

**Sebastian Degaudenci**
