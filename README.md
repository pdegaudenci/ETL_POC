# Prueba Técnica – Python + GCP + BigQuery + Cloud Composer

## Descripción

Proyecto de ingeniería de datos con dos implementaciones complementarias.

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
BigQuery SANDBOX
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
MERGE / UPSERT
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
│   ├── sql/
│   ├── output/
│   ├── tests/
│   └── requirements.txt
│
├── version_airflow/
│   ├── dags/
│   │   ├── coingecko_market_pipeline_dag.py
│   │   ├── utils/
│   │   └── sql/
│   └── requirements.txt
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
* Cloud Storage API habilitada
* Kubernetes Engine API habilitada
* Cuenta de servicio IAM
* Clave JSON descargada

---

## Configuración GCP

### Crear proyecto

```text
etl-poc-494716
```

### Activar APIs

* BigQuery API
* Cloud Composer API
* Cloud Storage API
* IAM API
* Kubernetes Engine API
* Compute Engine API

---

## Cuenta de Servicio

Crear:

```text
composer-etl-sa
```

---

## Roles IAM recomendados

### Para el usuario (UI Composer)

* Composer User
* Viewer

Opcional:

* Composer Administrator

### Para la Service Account Composer

* Composer Worker
* BigQuery Data Editor
* BigQuery Job User
* Storage Object Viewer
* Storage Object Creator

### Roles adicionales indicados por error de creación

* roles/editor
* roles/composer.ServiceAgentV2Ext

### Opción rápida demo

* BigQuery Admin
* Storage Admin
* Composer Administrator

---

## Descargar clave JSON

```text
credentials/service_account.json
```

---

## Variables de entorno (.env) – Versión Base

```env
PROJECT_ID=etl-poc-494716
BQ_LOCATION=EU

APP_NAME=CRYPTO

SANDBOX_DATASET=SANDBOX_CRYPTO
SANDBOX_TABLE=coingecko_markets

INTEGRATION_DATASET=INTEGRATION
INTEGRATION_TABLE=integration_prueba_tecnica

API_URL=https://api.coingecko.com/api/v3/coins/markets
API_VS_CURRENCY=usd
API_ORDER=market_cap_desc
API_LIMIT=100
API_PAGE=1
API_SPARKLINE=false

SOURCE_NAME=coingecko
WRITE_DISPOSITION=WRITE_TRUNCATE

GOOGLE_APPLICATION_CREDENTIALS=credentials/service_account.json
```

---

## Variables de entorno – Cloud Composer

```env
PROJECT_ID1=etl-poc-494716
BQ_LOCATION=EU

APP_NAME=PRUEBA_CRYPTO

SANDBOX_DATASET=SANDBOX_PRUEBA_CRYPTO
SANDBOX_TABLE=coingecko_markets_raw

INTEGRATION_DATASET=INTEGRATION
INTEGRATION_TABLE=integration_coingecko_markets

API_URL=https://api.coingecko.com/api/v3/coins/markets
API_VS_CURRENCY=usd
API_ORDER=market_cap_desc
API_LIMIT=100
API_PAGE=1
API_SPARKLINE=false

SOURCE_NAME=coingecko
WRITE_DISPOSITION=WRITE_TRUNCATE
```

En `config.py`:

```python
PROJECT_ID = os.getenv("PROJECT_ID1")
```

---

## Instalación – Versión Base

```bash
pip install -r requirements.txt
```

---

## Ejecución – Versión Base

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

* ingestion_date
* ingested_at

Optimización:

* Particionado por `ingestion_date`
* Clustering por `symbol`

### INTEGRATION

Optimización:

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
* Crea tabla si no existe

---

## Data Quality Checks

Archivo:

```text
sql/data_quality_checks.sql
```

Incluye:

* Conteo de registros
* Nulos
* Duplicados
* Rangos inválidos
* Reconciliación SANDBOX vs INTEGRATION
* Top market cap
* Resumen final

---

## Tests

```bash
python -m pytest
```

```bash
python -m pytest tests/integration/test_real_api.py -s
```

---

## Cloud Composer – Despliegue paso a paso

### 1. Crear entorno

```text
Cloud Composer → Create Environment
```

Configurar:

```text
Name: composer-coingecko-dev
Region: europe-west1
Composer Version: Composer 3
Environment Size: Small
Network: default
Service Account: composer-etl-sa
Web server access: All IP addresses
```

### 2. PyPI Packages

Dejar vacío o solo:

```text
requests==2.32.3
```

No instalar:

```text
google-cloud-bigquery
```

### 3. Subir DAGs al bucket

Subir contenido de:

```text
version_airflow/dags/
```

Debe quedar:

```text
dags/
├── coingecko_market_pipeline_dag.py
├── utils/
└── sql/
```

### 4. Esperar carga del DAG

```text
coingecko_market_pipeline
```

### 5. Activar DAG

* Toggle ON
* Unpause

### 6. Ejecutar DAG

```text
Trigger DAG
```

### 7. Orden esperado de tareas

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
check_no_duplicate_business_key
run_data_quality_summary
```

---

## Troubleshooting

### Error 403 Airflow UI

Revisar:

* Roles IAM del usuario
* Web server access control
* Proyecto correcto
* Cuenta Google correcta

### Broken DAG

Revisar:

* Variables de entorno
* Imports
* Rutas SQL `dags/sql/...`

---

## Evidencia

![Tablas BigQuery](https://raw.githubusercontent.com/pdegaudenci/ETL_POC/master/version_base/docs/tablas.png)

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
