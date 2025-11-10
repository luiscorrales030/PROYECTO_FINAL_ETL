# Proyecto Final: Pipeline ETL de Riesgo Hipotecario y Quiebras Bancarias

Este proyecto implementa un pipeline ETL completo orquestado con Apache Airflow,
utilizando Docker para una reproducibilidad total.

El pipeline ingesta datos sobre riesgo hipotecario y quiebras bancarias en EE.UU.,
los transforma, valida y carga en un Data Warehouse (Postgres) y un artefacto
Parquet para análisis.

## Stack Tecnológico

* **Orquestación:** Apache Airflow
* **Contenerización:** Docker & Docker Compose
* **Procesamiento:** Python, Pandas
* **Validación:** Great Expectations
* **Data Warehouse:** PostgreSQL


## Requisitos Previos

* Docker
* Docker Compose

## Ejecución del Proyecto

### 1. Construir la Imagen de Docker

Antes de levantar los servicios, debemos construir nuestra imagen de Airflow
personalizada que incluye `pandas`, `great-expectations`, etc.

```bash
# Opcional: define un nombre para la imagen
export AIRFLOW_IMAGE_NAME=proyecto-airflow-custom:latest

# Construye la imagen
docker build -t $AIRFLOW_IMAGE_NAME .

```

### 2. Colocar los Datos de Entrada
Asegúrate de que los archivos fuente estén en la carpeta data/input/:

data/input/failed_banks_raw.json

data/input/mortgage_risk_per_states.csv

### 3. Levantar los Servicios
Con la imagen construida y los datos en su lugar, levanta todo el stack:

Bash

docker compose up -d
Esto iniciará:

airflow-webserver (UI): http://localhost:8080 (user: admin, pass: admin)

airflow-scheduler: El motor que ejecuta los DAGs.

airflow_postgres: Base de datos del backend de Airflow.

dw_postgres: El Data Warehouse (expuesto en localhost:5434 para inspección).

### 4. Ejecutar el DAG
Abre la UI de Airflow en http://localhost:8080.

Busca el DAG main_etl_dag.

Activa el DAG (moviendo el interruptor de "pausa" a "activo").

Para una ejecución inmediata, presiona el botón "Play" (Trigger DAG).

Verificación de Resultados
Logs de Airflow: Inspecciona los logs de cada tarea en la UI. Verás los conteos de filas leídas, transformadas y descartadas.

Artefacto Parquet: El resultado final se guardará en:

data/output/processed/final_analytics_table.parquet

Reporte de Validación: El reporte de Great Expectations se genera en:

data/output/ge_reports/validation_report.html

Data Warehouse (Postgres): Puedes conectarte al DW (en localhost:5434, BD: dwh, User: dwh_user, Pass: dwh_pass) y consultar la tabla:

SQL

SELECT * FROM analytics.risk_and_failures LIMIT 10;