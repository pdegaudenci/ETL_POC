"""
ETL package.

Este paquete contiene las capas principales del pipeline:

- extract: descarga datos desde una API pública.
- load: carga datos crudos en BigQuery SANDBOX.
- transform: ejecuta SQL para publicar datos en INTEGRATION.
"""