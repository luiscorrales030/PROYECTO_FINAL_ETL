# Usamos una imagen oficial de Airflow
# Dockerfile
FROM apache/airflow:2.7.3

# Instalar dependencias del sistema
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Cambiar de vuelta al usuario airflow
USER airflow

# Copiar e instalar requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt