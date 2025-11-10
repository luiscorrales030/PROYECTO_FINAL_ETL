import pandas as pd
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

PROCESSED_PATH = "/opt/airflow/data/output/processed"

def load_data(ti=None):
    """
    Carga el DataFrame validado en el Data Warehouse (Postgres)
    y como un artefacto Parquet.
    """
    validated_file = ti.xcom_pull(task_ids='validate_data')
    
    logging.info(f"Iniciando carga de datos desde {validated_file}")
    
    df = pd.read_parquet(validated_file)
    
    # --- Carga 1: Artefacto Parquet ---
    artifact_path = f"{PROCESSED_PATH}/final_analytics_table.parquet"
    df.to_parquet(artifact_path, index=False)
    logging.info(f"Artefacto guardado exitosamente en {artifact_path}")
    
    # --- Carga 2: Data Warehouse (Postgres) ---
    postgres_hook = PostgresHook(postgres_conn_id="dw_postgres_conn")
    
    # Usamos el motor de SQLAlchemy para un 'to_sql' eficiente
    engine = postgres_hook.get_sqlalchemy_engine()
    
    schema = "analytics"
    table = "risk_and_failures"
    
    try:
        logging.info(f"Cargando {len(df)} filas en {schema}.{table}...")
        
        # 'if_exists='replace'' asegura la idempotencia
        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists='replace',
            index=False,
            method='multi' # Eficiente para inserción
        )
        
        logging.info(f"Carga a Postgres finalizada exitosamente.")
        
    except Exception as e:
        logging.error(f"Error durante la carga a Postgres: {e}")
        raise