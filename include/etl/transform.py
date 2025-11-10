import pandas as pd
import logging

STAGING_PATH = "/opt/airflow/data/output/staging"
TRANSFORMED_PATH = "/opt/airflow/data/output/transformed"

def _transform_banks(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma el DataFrame de bancos crudos."""
    logging.info(f"Transformando {len(df)} registros de bancos...")
    
    # 1. Convertir 'closing_date' a datetime
    # El formato es dd-Mon-yy (ej. 07-Sep-01)
    df['closing_date'] = pd.to_datetime(df['closing_date'], format='%d-%b-%y')
    
    # 2. Extraer año
    df['closing_year'] = df['closing_date'].dt.year
    
    # 3. Filtrar por el rango de interés (basado en el ejemplo)
    df_filtered = df[(df['closing_year'] >= 2000) & (df['closing_year'] <= 2025)].copy()
    
    # 4. Agregar: Contar quiebras por estado y año
    df_agg = df_filtered.groupby(['state', 'closing_year']).size().reset_index(name='failed_banks_count')
    
    logging.info(f"Transformación de bancos finalizada. {len(df_agg)} registros agregados.")
    return df_agg

def _transform_mortgage(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma el DataFrame de hipotecas (filtra, limpia, pivota)."""
    logging.info(f"Transformando {len(df)} registros de hipotecas...")
    
    # 1. Métricas de interés (basado en el ejemplo y snippets)
    desired_stats = ['Average DTI', 'Average Shocked Stressed Default Rate']
    
    # 2. Filtrar por métricas y propósito
    df_filtered = df[
        (df['Statistic'].isin(desired_stats)) &
        (df['Loan purpose'] == 'All loans')
    ].copy()
    
    logging.info(f"Filtrados {len(df_filtered)} registros de hipotecas por estadísticas de interés.")
    
    # 3. Seleccionar columnas relevantes
    df_clean = df_filtered[['State', 'Year', 'Statistic', 'Total']]
    
    # 4. Limpiar la columna 'Total' (ej. '35,5%' -> 35.5)
    df_clean['value'] = df_clean['Total'].str.replace('%', '', regex=False) \
                                        .str.replace(',', '.', regex=False)
    
    # Manejar 'NR' (Not Reported) u otros no numéricos
    df_clean['value'] = pd.to_numeric(df_clean['value'], errors='coerce')
    
    # 5. Pivotar la tabla
    df_pivot = df_clean.pivot_table(
        index=['State', 'Year'],
        columns='Statistic',
        values='value'
    ).reset_index()
    
    # 6. Renombrar columnas para que sean amigables
    df_pivot.columns = [
        'state', 'year', 'avg_dti', 'avg_shocked_default_rate'
    ]
    
    logging.info(f"Pivoteo de hipotecas finalizado. {len(df_pivot)} registros generados.")
    return df_pivot


def transform_data(ti=None) -> str:
    """
    Tarea principal de transformación.
    Lee los archivos de staging, los transforma y los une.
    """
    # Obtener rutas de XCom
    banks_staging_file = ti.xcom_pull(task_ids='extract_failed_banks')
    mortgage_staging_file = ti.xcom_pull(task_ids='extract_mortgage_risk')
    
    output_file = f"{TRANSFORMED_PATH}/transformed_analytics_data.parquet"
    
    # Cargar datos
    df_banks_raw = pd.read_parquet(banks_staging_file)
    df_mortgage_raw = pd.read_parquet(mortgage_staging_file)
    
    # Transformar
    df_banks_agg = _transform_banks(df_banks_raw)
    df_mortgage_pivot = _transform_mortgage(df_mortgage_raw)
    
    # Unir (Join)
    # Usamos un 'left' join desde la tabla de hipotecas (nuestra base analítica)
    # a la tabla de quiebras.
    df_final = pd.merge(
        df_mortgage_pivot,
        df_banks_agg,
        how='left',
        left_on=['state', 'year'],
        right_on=['state', 'closing_year']
    )
    
    # Post-procesamiento del Join
    # Rellenar con 0 bancos quebrados en los años/estados donde no hubo
    df_final['failed_banks_count'] = df_final['failed_banks_count'].fillna(0).astype(int)
    # Eliminar columna 'closing_year' redundante
    df_final = df_final.drop(columns=['closing_year'])
    
    logging.info(f"Join finalizado. {len(df_final)} registros en la tabla analítica.")
    
    # Guardar archivo transformado
    df_final.to_parquet(output_file, index=False)
    logging.info(f"Datos transformados guardados en: {output_file}")
    
    return output_file