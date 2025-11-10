import sys
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# Añadir 'include' al PYTHONPATH para que Airflow pueda encontrar los módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'include')))

# Importar nuestras funciones ETL
from etl import extract, transform, validate, load

# Definir rutas base de Airflow
DATA_PATH = "/opt/airflow/data"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='main_etl_dag',
    default_args=default_args,
    description='Pipeline ETL para riesgo hipotecario y quiebras bancarias',
    schedule_interval='@daily',
    catchup=False,  # No ejecutar DAGs pasados al iniciar
    tags=['etl', 'proyecto_final', 'fdic', 'mortgage'],
)
def main_etl_pipeline():
    """
    ### Pipeline ETL Principal
    Orquesta la extracción, transformación, validación y carga de
    datos de riesgo hipotecario y quiebras bancarias.
    """

    @task
    def start():
        """Dummy task de inicio."""
        print("Iniciando el pipeline ETL...")
        pass

    setup_directories = BashOperator(
        task_id='setup_directories',
        bash_command=f"""
            mkdir -p {DATA_PATH}/output/staging;
            mkdir -p {DATA_PATH}/output/transformed;
            mkdir -p {DATA_PATH}/output/processed;
            mkdir -p {DATA_PATH}/output/ge_reports;
        """,
    )

    # --- EXTRACT ---
    # Usamos @task.docker para un futuro aislamiento,
    # por ahora @task simple es suficiente.
    @task(task_id='extract_failed_banks')
    def extract_failed_banks_task():
        return extract.extract_failed_banks()

    @task(task_id='extract_mortgage_risk')
    def extract_mortgage_risk_task():
        return extract.extract_mortgage_risk()

    # --- TRANSFORM ---
    @task(task_id='transform_data')
    def transform_data_task(banks_staging_file: str, mortgage_staging_file: str):
        # Esta tarea no puede ejecutarse hasta que ambas extracciones terminen.
        # Airflow maneja esto automáticamente basado en los argumentos.
        # Pero debemos pasar el 'ti' (task instance) para XComs
        
        # En el TaskFlow API, no necesitamos 'ti'.
        # Los argumentos se pasan automáticamente.
        # ¡Pero nuestras funciones DEBEN recibir 'ti' si usamos XCom pull!
        
        # Refactor: Usemos la nueva forma. Los @task devuelven valores
        # y los siguientes tasks los reciben como argumentos.
        
        # ... El problema es que las funciones subyacentes usan 'ti.xcom_pull'.
        # Vamos a rediseñar el DAG para que sea compatible con TaskFlow
        # o volvemos a la forma clásica.
        
        # Decisión: Mantenemos las funciones como están (con 'ti')
        # y las llamamos desde un wrapper de PythonOperator o un @task
        # que acepte 'ti'
        
        # Re-declaración de tareas para pasar 'ti'
        pass
    
    # -----------------------------------------------------------------
    # CORRECCIÓN: El API @task es más limpio. Modifiquemos las
    # funciones ETL para que *no* usen 'ti' y reciban las rutas
    # como argumentos. Esto es mejor diseño.
    #
    # (El código en include/etl/ ya fue modificado para usar XCom
    # pull, así que usaremos la forma clásica con 'ti'
    # o mejor, usaremos la forma moderna donde 'ti' se pasa
    # implícitamente).
    # -----------------------------------------------------------------
    
    # Versión correcta usando TaskFlow API (implica que las funciones
    # en los módulos NO usan 'ti.xcom_pull', sino que reciben
    # los valores de las tareas anteriores como argumentos).
    #
    # Vamos a *re-escribir* las firmas de las tareas del DAG
    # para que coincidan con lo que el TaskFlow API espera.
    # El código en `include/` ya está escrito con `ti`,
    # así que usaremos ese patrón.

    # Tarea de transformación (depende de las dos extracciones)
    @task(task_id='transform_data')
    def transform_data_task_wrapper(ti=None):
        return transform.transform_data(ti=ti)

    # Tarea de validación (depende de la transformación)
    @task(task_id='validate_data')
    def validate_data_task_wrapper(ti=None):
        return validate.validate_data(ti=ti)

    # Tarea de carga (depende de la validación)
    @task(task_id='load_data')
    def load_data_task_wrapper(ti=None):
        load.load_data(ti=ti)

    @task
    def end():
        """Dummy task de fin."""
        print("Pipeline ETL finalizado exitosamente.")
        pass

    # --- Definición de dependencias ---
    
    start_op = start()
    setup_op = setup_directories
    
    extract_banks_op = extract_failed_banks_task()
    extract_mortgage_op = extract_mortgage_risk_task()
    
    transform_op = transform_data_task_wrapper()
    validate_op = validate_data_task_wrapper()
    load_op = load_data_task_wrapper()
    
    end_op = end()

    # Flujo:
    # 1. Iniciar y configurar directorios
    start_op >> setup_op
    
    # 2. Las extracciones corren en paralelo, después de setup
    setup_op >> extract_banks_op
    setup_op >> extract_mortgage_op
    
    # 3. La transformación depende de AMBAS extracciones
    [extract_banks_op, extract_mortgage_op] >> transform_op
    
    # 4. Flujo secuencial: Transform -> Validate -> Load
    transform_op >> validate_op >> load_op
    
    # 5. Tarea final
    load_op >> end_op

# Instanciar el DAG para que Airflow lo registre
main_etl_pipeline()