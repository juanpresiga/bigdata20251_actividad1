import pandas as pd
import numpy as np
from typing import Dict, Optional
from static.db.config import create_connection, create_table
from utils.helpers import log_step, get_output_txt, create_file

class DataEnricher:
    def __init__(self):
        self.config = {
            'name_db': 'bd_analisis_name.sqlite',
            'symbol_db': 'bd_analisis_symbol.sqlite',
            'name_table': 'df_name_clean',
            'symbol_table': 'df_symbol_clean',
            'merged_table': 'df_merged',
            'key_mappings': {
                'name': 'name',
                'symbol': 'symbol'
            },
            'numeric_columns': [
                'maxSupply', 'supply', 'marketCapUsd', 'volumeUsd24Hr',
                'priceUsd', 'changePercent24Hr', 'vwap24Hr'
            ]
        }

        self.log_path = 'src/static/enrichment_report/enrichment_report.txt'

    def load_data(self, db_file: str, table: str) -> pd.DataFrame:
        """Carga datos desde una tabla SQLite."""
        conn = create_connection(db_file)
        try:
            query = f"SELECT * FROM {table}"
            df = pd.read_sql_query(query, conn)
            log_step(f"Datos cargados de {table}. Forma: {df.shape}\n", self.log_path)
            return df
        finally:
            conn.close()

    def merge_dataframes(self, df_name: pd.DataFrame, df_symbol: pd.DataFrame) -> pd.DataFrame:
        """Combina los DataFrames de name y symbol."""
        # Verificar columnas disponibles
        log_step("Columnas en df_name: " + ", ".join(df_name.columns) + "\n", self.log_path)
        log_step("Columnas en df_symbol: " + ", ".join(df_symbol.columns) + "\n", self.log_path)

        # Realizar el merge basado en la relación entre name y symbol
        # Primero intentamos hacer el merge por name si está disponible
        if 'id' in df_name.columns and 'id' in df_symbol.columns:
            merged_df = pd.merge(
                df_name, 
                df_symbol,
                on='id',
                how='outer',
                suffixes=('_name', '_symbol')
            )
        # Si no hay id name, intentamos por symbol
        elif 'name' in df_name.columns and 'name' in df_symbol.columns:
            merged_df = pd.merge(
                df_name, 
                df_symbol,
                on='name',
                how='outer',
                suffixes=('_name', '_symbol')
            )
        else:
            raise ValueError("No se encontraron columnas comunes para hacer el merge")

        log_step(f"Merge completado. Forma resultante: {merged_df.shape}\n", self.log_path)
        return merged_df

    def normalize_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.config['numeric_columns']:
            if col in df.columns:
                # Convertir a numérico, reemplazando errores con NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Rellenar NaN con 0
                df[col] = df[col].fillna(0)
                
        return df

    def handle_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Maneja duplicados en el DataFrame combinado."""
        # Identificar duplicados
        duplicates = df.duplicated(subset=['name', 'symbol'], keep=False)
        if duplicates.any():
            log_step(f"Se encontraron {duplicates.sum()} filas duplicadas\n", self.log_path)
            
            # Para cada conjunto de duplicados, mantener el registro con más información
            df['info_count'] = df.notna().sum(axis=1)
            df = df.sort_values('info_count', ascending=False).drop_duplicates(
                subset=['name', 'symbol'], keep='first'
            )
            df = df.drop('info_count', axis=1)
            
            log_step("Duplicados manejados\n", self.log_path)
        
        return df

    def save_results(self, df: pd.DataFrame):
        """Guarda los resultados del enriquecimiento."""
        # Guardar en CSV
        output_path = 'src/static/enriched_data'
        create_file(df, f'{output_path}/enriched_data.csv', file_format='csv')
        create_file(df, f'{output_path}/enriched_data.xlsx', file_format='xlsx')
        log_step("Archivos de datos enriquecidos guardados\n", self.log_path)
        
        # Guardar en SQLite
        conn = create_connection('bd_analisis_enriched.sqlite')
        try:
            create_table(conn, self.config['merged_table'], df)
            log_step("Datos guardados en SQLite\n", self.log_path)
        finally:
            conn.close()

    def process(self):
        """Ejecuta el proceso completo de enriquecimiento de datos."""
        try:
            log_step("Iniciando proceso de enriquecimiento de datos\n", self.log_path)
            
            # Cargar datos limpios
            df_name = self.load_data(self.config['name_db'], self.config['name_table'])
            df_symbol = self.load_data(self.config['symbol_db'], self.config['symbol_table'])
            
            # Combinar datos
            merged_df = self.merge_dataframes(df_name, df_symbol)
            
            # Manejar duplicados
            merged_df = self.handle_duplicates(merged_df)
            
            # Normalizar datos numéricos
            merged_df = self.normalize_numeric_columns(merged_df)
            
            # Guardar resultados
            self.save_results(merged_df)
            
            log_step("Proceso de enriquecimiento completado exitosamente\n", self.log_path)
            return merged_df
            
        except Exception as e:
            log_step(f"Error durante el enriquecimiento de datos: {str(e)}\n", self.log_path)
            raise

def main():
    """Función principal para ejecutar el enriquecimiento de datos."""
    enricher = DataEnricher()
    enricher.process()

if __name__ == "__main__":
    main()
