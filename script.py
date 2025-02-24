import pandas as pd
import json
import os
from dotenv import load_dotenv
from api.apiBase import fetch_data_from_api
from database.config import create_connection

load_dotenv()

def main():
    api_url = 'https://api.coincap.io/v2/assets'
    
    data = fetch_data_from_api(api_url)
    
    create_json_file(data)

    if isinstance(data, dict):
        data = [data]

    # df_api = pd.DataFrame(data)
    df_api = pd.json_normalize(data, record_path=['data'], meta=['timestamp'])
    print(df_api)
    create_xlsx_file(df_api)
 
    if os.getenv('DB_CONNECTION'):
        conn = create_connection()
        if conn:
            df_api = df_api.applymap(lambda x: str(x) if isinstance(x, list) else x)
            df_api.to_sql('table_name', conn, if_exists='replace', index=False)
            print("Datos insertados en la base de datos exitosamente.")
            conn.close()

def create_json_file(data):
    with open('request_api_json.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)
    print("Archivo JSON 'request_api_json.json' generado exitosamente.")

def create_xlsx_file(df):
    df.to_excel('request_api_xlsx.xlsx', index=False)
    print("Archivo Excel 'request_api_xlsx.xlsx' generado exitosamente.")

if __name__ == '__main__':
    main()