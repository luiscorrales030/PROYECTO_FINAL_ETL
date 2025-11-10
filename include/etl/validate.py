import great_expectations as ge
from great_expectations.core.batch import Batch
import logging
import pandas as pd
import os
import great_expectations.render as render

TRANSFORMED_PATH = "/opt/airflow/data/output/transformed"
GE_PROJECT_PATH = "/opt/airflow/great_expectations"
GE_REPORTS_PATH = "/opt/airflow/data/output/ge_reports"

# Asegurarse de que el directorio de reportes exista
os.makedirs(GE_REPORTS_PATH, exist_ok=True)

def validate_data(ti=None) -> str:
    """
    Valida el DataFrame transformado usando la API V2 (0.15.x) de Great Expectations.
    Falla la tarea si la validación no es exitosa.
    """
    transformed_file = ti.xcom_pull(task_ids='transform_data')
    
    logging.info(f"Iniciando validación de datos (GE V2/0.15.x) para {transformed_file}")
    
    # 1. Cargar el contexto (V2)
    # Esto funciona en la 0.15.x sin necesidad de ge.get_context()
    context = ge.DataContext()
    
    # 2. Definir la suite (V2)
    suite_name = "analytics_suite"
    context.create_expectation_suite(suite_name, overwrite_existing=True)
    suite = context.get_expectation_suite(suite_name)

    # 3. Añadir expectativas (V2)
    logging.info("Añadiendo expectativas V2...")
    suite.add_expectation(
        ge.core.ExpectationConfiguration("expect_table_row_count_to_be_between", kwargs={"min_value": 1})
    )
    suite.add_expectation(
        ge.core.ExpectationConfiguration("expect_column_values_to_not_be_null", kwargs={"column": "state"})
    )
    suite.add_expectation(
        ge.core.ExpectationConfiguration("expect_column_values_to_be_between", kwargs={"column": "year", "min_value": 2000, "max_value": 2025})
    )
    suite.add_expectation(
        ge.core.ExpectationConfiguration("expect_column_values_to_be_of_type", kwargs={"column": "failed_banks_count", "type_": "int64"})
    )
    suite.add_expectation(
        ge.core.ExpectationConfiguration("expect_column_values_to_be_between", kwargs={"column": "failed_banks_count", "min_value": 0})
    )
    suite.add_expectation(
        ge.core.ExpectationConfiguration("expect_column_values_to_not_be_null", kwargs={"column": "avg_dti", "mostly": 0.95}) # 95% de no nulos
    )
    logging.info("Expectativas añadidas.")

    # 4. Cargar datos
    df = pd.read_parquet(transformed_file)
    
    # 5. Crear un batch (V2 para DataFrames en memoria)
    # Esta es la sintaxis simple de la V2 que fallaba en la V3
    batch = Batch(data=df)
    
    # 6. Ejecutar la validación (V2)
    validator = context.get_validator(batch=batch, expectation_suite=suite)
    validation_result = validator.validate()
    
    # 7. Renderizar HTML (V2)
    report_path = os.path.join(GE_REPORTS_PATH, "validation_report.html")
    document_model = render.RenderedDocumentContent.rendered_document_content(
        validation_result, data_docs_pages=None
    )
    with open(report_path, "w") as f:
        f.write(document_model.to_html())
    
    logging.info(f"Reporte de validación guardado en {report_path}")
    
    # 8. Comprobar éxito
    if not validation_result["success"]:
        logging.error(f"Validación de Great Expectations fallida: {validation_result}")
        raise ValueError("La validación de datos falló. Revisar el reporte para más detalles.")
        
    logging.info("Validación de Great Expectations exitosa.")
    
    return transformed_file