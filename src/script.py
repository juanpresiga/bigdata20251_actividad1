import pandas as pd
import os
import time
import sys
#from dotenv import load_dotenv    
from api.apiBase import fetch_data_from_api
from static.db.config  import create_connection, create_table
from utils.helpers import  audit_data, create_file

#load_dotenv()

def main():

    api_url = api_url = 'https://api.coincap.io/v2/assets'
    
    
    time.sleep(2)
    data = fetch_data_from_api(api_url)

    df_api = pd.json_normalize(data, record_path=['data'], meta=['timestamp'])
    df_api_name = df_api[['id', 'name', 'rank', 'supply', 'explorer' , 'marketCapUsd']]
    df_api_symbol = df_api[['id', 'symbol', 'maxSupply', 'priceUsd', 'volumeUsd24Hr', 'changePercent24Hr', 'vwap24Hr', 'timestamp']]

    audit_data(df_api_symbol)
    audit_data(df_api_name)

    create_file(df_api, 'src/static/xlsx/request_api_xlsx.xlsx', file_format='xlsx')
    create_file(df_api_symbol, 'src/static/xlsx/df_api_symbol_xlsx.xlsx', file_format='xlsx')
    create_file(df_api_name, 'src/static/xlsx/df_api_name_xlsx.xlsx', file_format='xlsx')

    try:
        conn_name = create_connection('bd_analisis_name.sqlite')
        conn_symbol = create_connection('bd_analisis_symbol.sqlite')
        create_table(conn_symbol, table_name='df_symbol', data=df_api_symbol)
        create_table(conn_name, table_name='df_name', data=df_api_name)
    except Exception as e:
        print(f"Error en la base de datos: {str(e)}")
    finally:
        if conn_name:
            conn_name.close()
            print("Conexión a la base de datos cerrada.")
        elif conn_symbol:
            conn_symbol.close()
            print("Conexión a la base de datos cerrada.")

if __name__ == '__main__':
    main()