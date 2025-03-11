import pandas as pd
import sqlite3
import os


def create_connection(db_name:str):
    db_path = os.path.join(os.curdir,db_name)
    connection = None
    try:
        connection = sqlite3.connect(db_path)
        print("Connection to SQLite DB successful")
    except sqlite3.Error as e:
        print(f"The error '{e}' occurred")
    return connection

def create_table(conn, table_name:str, data:list):
    if isinstance(data, dict):
        data = [data]

    # data = pd.json_normalize(data, record_path=['data'], meta=['timestamp'])
    if conn:
        df_api = data.applymap(lambda x: str(x) if isinstance(x, list) else x)
        df_api.to_sql(table_name, conn, if_exists='replace', index=False)
        print("Datos insertados en la base de datos exitosamente.")
        conn.close()