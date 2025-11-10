# Usamos una imagen oficial de Airflow
FROM apache/airflow:2.8.1


# Cambiamos a root para instalar dependencias del sistema si fueran necesarias
USER root
# (Aquí podríamos instalar librerías de sistema si 'psycopg2' fallara)

# Volvemos al usuario airflow
USER airflow

# Copiamos el archivo de requerimientos
COPY requirements.txt /

# Instalamos las dependencias de Python
RUN pip install --no-cache-dir -r /requirements.txt