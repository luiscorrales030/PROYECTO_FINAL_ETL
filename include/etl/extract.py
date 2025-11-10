import pandas as pd
import logging
import io
import csv
from typing import List, Dict, Any

# Definir rutas basadas en la estructura de Airflow
BASE_DATA_PATH = "/opt/airflow/data"
INPUT_PATH = f"{BASE_DATA_PATH}/input"
STAGING_PATH = f"{BASE_DATA_PATH}/output/staging" # Usamos staging para archivos intermedios

def extract_failed_banks(ti=None) -> str:
    """
    Lee el JSON de bancos, parsea el problemático string 'raw'
    y guarda el resultado en un archivo parquet de staging.
    """
    source_file = f"{INPUT_PATH}/failed_banks_raw.json"
    output_file = f"{STAGING_PATH}/staging_banks.parquet"
    
    logging.info(f"Iniciando extracción de {source_file}")
    
    try:
        raw_df = pd.read_json(source_file)
        logging.info(f"Leídas {len(raw_df)} líneas JSON en crudo.")
    except Exception as e:
        logging.error(f"Error al leer el archivo JSON {source_file}: {e}")
        raise
        
    parsed_data: List[List[str]] = []
    discarded_count = 0
    
    # Definimos el esquema esperado del CSV en el string 'raw'
    # Bank Name, City, State, Cert, Acquiring Institution, Closing Date, Fund
    expected_columns = 7 
    
    for index, row in raw_df.iterrows():
        raw_line = row['raw']
        
        # 1. Limpieza del string
        # Elimina \ " al inicio y al final
        cleaned_line = raw_line.strip().lstrip('\\"').rstrip('\\"')
        # Reemplaza dobles escapes internos por un solo escape (para el parser CSV)
        cleaned_line = cleaned_line.replace('\\"\\"', '""')
        # Reemplaza escapes simples (problemáticos)
        cleaned_line = cleaned_line.replace('\\"', '"')

        # 2. Parseo con el módulo CSV
        f = io.StringIO(cleaned_line)
        reader = csv.reader(f, delimiter=',', quotechar='"')
        
        try:
            parsed_row = next(reader)
            if len(parsed_row) == expected_columns:
                parsed_data.append(parsed_row)
            else:
                logging.warning(f"Línea {row['line_number']} descartada. Se esperaban {expected_columns} columnas, se obtuvieron {len(parsed_row)}: {raw_line}")
                discarded_count += 1
        except Exception as e:
            logging.error(f"Error parseando línea {row['line_number']}: {e}. Línea: {raw_line}")
            discarded_count += 1
            
    # 3. Crear DataFrame final
    df_banks = pd.DataFrame(parsed_data, columns=[
        'bank_name', 'city', 'state', 'cert', 'acq_institution', 'closing_date', 'fund'
    ])
    
    logging.info(f"Extracción de bancos completada. {len(df_banks)} registros leídos, {discarded_count} descartados.")
    
    # 4. Guardar en staging
    df_banks.to_parquet(output_file, index=False)
    logging.info(f"Datos de bancos guardados en staging: {output_file}")
    
    # Devolver la ruta para el XCom
    return output_file


def extract_mortgage_risk(ti=None) -> str:
    """
    Lee el CSV de riesgo hipotecario (delimitado por ';')
    y lo guarda en un archivo parquet de staging.
    """
    source_file = f"{INPUT_PATH}/mortgage_risk_per_states.csv"
    output_file = f"{STAGING_PATH}/staging_mortgage.parquet"

    logging.info(f"Iniciando extracción de {source_file}")
    
    try:
        df_mortgage = pd.read_csv(source_file, delimiter=';')
        logging.info(f"Leídas {len(df_mortgage)} filas del CSV de hipotecas.")
    except Exception as e:
        logging.error(f"Error al leer el archivo CSV {source_file}: {e}")
        raise
        
    # Guardar en staging
    df_mortgage.to_parquet(output_file, index=False)
    logging.info(f"Datos de hipotecas guardados en staging: {output_file}")
    
    return output_file