import pandas as pd
import os
from dotenv import load_dotenv
from api.apiBase import fetch_data_from_api
from static.db.config  import create_connection
from utils.helpers import  create_xlsx_file, audit_data

load_dotenv()

def main():

    api_url = 'https://api.coinlore.net/api/tickers/'
    
    data = fetch_data_from_api(api_url)


    if isinstance(data, dict):
        data = [data]

    df_api = pd.json_normalize(data, record_path=['data'])

    audit_data(df_api)

    create_xlsx_file(df_api)

    #audit_data_md(df_api)
 

    conn = create_connection()
    if conn:
        df_api = df_api.applymap(lambda x: str(x) if isinstance(x, list) else x)
        df_api.to_sql('table_name', conn, if_exists='replace', index=False)
        print("Datos insertados en la base de datos exitosamente.")
        conn.close()

if __name__ == '__main__':
    main()