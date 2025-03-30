import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from static.db.config import create_connection, create_table
from utils.helpers import (
    create_file, format_number, log_step, 
    get_output_txt, convert_to_numeric, convert_to_lowercase
)

class DataCleaner:
    def __init__(self):
        self.config = {
            'name': {
                'db_file': 'bd_analisis_name.sqlite',
                'table': 'df_name',
                'clean_table': 'df_name_clean',
                'numeric_columns': ['maxSupply', 'supply', 'marketCapUsd', 'volumeUsd24Hr', 
                                  'priceUsd', 'changePercent24Hr', 'vwap24Hr'],
                'string_columns': ['name'],
                'fill_values': {
                    'maxSupply': 0, 
                    'supply': 0,
                    'marketCapUsd': 0,
                    'volumeUsd24Hr': 0,
                    'priceUsd': 0,
                    'changePercent24Hr': 0,
                    'vwap24Hr': 0,
                    'explorer': 'Sin datos'
                }
            },
            'symbol': {
                'db_file': 'bd_analisis_symbol.sqlite',
                'table': 'df_symbol',
                'clean_table': 'df_symbol_clean',
                'numeric_columns': ['supply', 'marketCapUsd'],
                'string_columns': [],
                'fill_values': {
                    'supply': 0,
                    'marketCapUsd': 0,
                    'explorer': 'Sin datos'
                }
            }
        }

    def connect_to_db(self, db_file: str) -> Tuple[object, object]:
        """Establece conexión con la base de datos."""
        conn = create_connection(db_file)
        cursor = conn.cursor()
        return conn, cursor

    def load_data(self, cursor: object, table: str) -> pd.DataFrame:
        """Carga datos desde la base de datos en un DataFrame."""
        cursor.execute(f"SELECT * FROM {table}")
        df = pd.DataFrame(cursor.fetchall(), columns=[x[0] for x in cursor.description])
        log_step(f"DataFrame {table} creado con forma: {df.shape}\n")
        return df

    def clean_dataframe(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Limpia un DataFrame según la configuración especificada."""
        df_log = get_output_txt(df, 'info')
        log_step(f"Información inicial del DataFrame:\n{df_log}\n")

        # Quita duplicados
        df = df.drop_duplicates()
        log_step(f"Duplicados eliminados. Nueva forma: {df.shape}\n")

        # Convierte columnas numericas
        available_numeric_cols = [col for col in config['numeric_columns'] if col in df.columns]
        if available_numeric_cols:
            convert_to_numeric(df, available_numeric_cols)
            log_step(f"Columnas convertidas a numéricas: {available_numeric_cols}\n")

        # Convierte columnas String a lowercase
        for col in config['string_columns']:
            if col in df.columns:
                convert_to_lowercase(df, [col])
                log_step(f"Columna {col} convertida a minúsculas\n")

        # Organiza valores null
        fill_values = {k: v for k, v in config['fill_values'].items() if k in df.columns}
        df = df.fillna(fill_values)
        log_step("Valores nulos rellenados\n")

        return df

    def save_results(self, df: pd.DataFrame, name: str, conn: object, config: Dict):
        """Guarda los resultados en archivos y base de datos."""
        # Guarda archivos CSV y Excel
        create_file(df, f'src/static/cleaned_data/cleaned_data_{name}.csv', file_format='csv')
        create_file(df, f'src/static/cleaned_data/cleaned_data_{name}.xlsx', file_format='xlsx')
        log_step(f"Archivos {name} guardados\n")

        # Guard BD
        create_table(conn, config['clean_table'], df)
        log_step(f"Tabla limpia {config['clean_table']} creada\n")

    def process_dataset(self, name: str):
        """Procesa un conjunto de datos completo."""
        config = self.config[name]
        conn, cursor = self.connect_to_db(config['db_file'])
        
        try:
            # Load data
            df = self.load_data(cursor, config['table'])
            
            # Clean data
            df_clean = self.clean_dataframe(df, config)
            
            # Save results
            self.save_results(df_clean, name, conn, config)
            
        finally:
            conn.close()

def main():
    """Función principal que ejecuta el proceso de limpieza."""
    log_step("Inicio proceso de limpieza de datos\n")
    
    cleaner = DataCleaner()
    
    try:
        # Process both datasets
        for dataset in ['name', 'symbol']:
            cleaner.process_dataset(dataset)
        
        log_step("Proceso de limpieza finalizado exitosamente\n")
        
    except Exception as e:
        log_step(f"Error durante el proceso de limpieza: {str(e)}\n")
        raise

if __name__ == "__main__":
    main()


