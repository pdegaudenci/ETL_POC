# Prueba Técnica – Python + GCP + BigQuery

## Descripción

Este proyecto implementa un pipeline sencillo de ingesta y transformación de datos utilizando **Python**, **Google Cloud Platform** y **BigQuery**.

El flujo realizado es:

1. Conexión a una API pública (**Open-Meteo**)
2. Descarga de 100 registros horarios
3. Carga de datos en BigQuery en un dataset con nomenclatura:

SANDBOX_PRUEBA_METEO

4. Transformación SQL desde SANDBOX hacia:

INTEGRATION.integration_prueba_tecnica

5. Transformación idempotente usando `MERGE` para evitar duplicados.

---

# Arquitectura

API Open-Meteo
      ↓
Python (ApiDownloader)
      ↓
BigQuery SANDBOX_PRUEBA_METEO.open_meteo_hourly
      ↓
sql/transform.sql
      ↓
BigQuery INTEGRATION.integration_prueba_tecnica

---

# Estructura del proyecto

EJERCICIO/
├── api/
│   ├── api_downloader.py
│   ├── bq_loader.py
│   └── main.py
├── credentials/
│   └── service_account.json
├── sql/
│   ├── transform.sql
│   └── data_quality_checks.sql
├── tests/
├── config.py
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md

---

# Prerrequisitos

## 1. Python

Instalar Python 3.10 o superior.

Verificar:

python --version

---

## 2. Cuenta de Google Cloud

Crear una cuenta en:

https://console.cloud.google.com/

Crear o seleccionar un proyecto y copiar el **Project ID**.

---

## 3. Activar BigQuery API

En Google Cloud Console:

APIs & Services → Library → BigQuery API → Enable

---

## 4. Crear Service Account

Ir a:

IAM & Admin → Service Accounts

Crear nueva cuenta:

prueba-tecnica-bigquery

---

## 5. Asignar Roles IAM

Asignar los siguientes roles:

BigQuery Data Editor
BigQuery Job User

Opcional:

BigQuery Admin

(útil para crear datasets automáticamente)

---

## 6. Descargar clave JSON

Dentro de la Service Account:

Keys → Add Key → Create New Key → JSON

Guardar el archivo en:

credentials/service_account.json

---

# Configuración de variables de entorno

Crear archivo `.env`

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

# Instalación

Instalar dependencias:

pip install -r requirements.txt

---

# Ejecución

Desde la raíz del proyecto:

python -m api.main

---

# Resultado esperado en BigQuery

## Dataset SANDBOX

SANDBOX_PRUEBA_METEO
└── open_meteo_hourly

## Dataset INTEGRATION

INTEGRATION
└── integration_prueba_tecnica

---

# SQL de transformación

Archivo:

sql/transform.sql

Características:

- Lee datos desde SANDBOX
- Elimina duplicados por `datetime`
- Añade fecha de ejecución
- Inserta en tabla final
- Es idempotente

---

# Validaciones de calidad de datos

Archivo:

sql/data_quality_checks.sql

Incluye consultas para:

- Conteo de registros
- Nulos
- Duplicados
- Reconciliación SANDBOX vs INTEGRATION

---

# Tests

Ejecutar tests:

python -m pytest

---

## Captura de pantalla

![Tablas BigQuery](https://github.com/pdegaudenci/ETL_POC/blob/master/docs/tablas.png)

---

# Tecnologías utilizadas

- Python
- Requests
- Google Cloud BigQuery
- SQL
- Pytest
- dotenv

---

# Autor

Prueba técnica desarrollada por Sebastián Degaudenci.
