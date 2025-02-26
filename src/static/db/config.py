import sqlite3
import os


db_path = os.path.join(os.curdir,'bd_analisis.sqlite')


def create_connection():
    connection = None
    try:
        connection = sqlite3.connect(db_path)
        print("Connection to SQLite DB successful")
    except sqlite3.Error as e:
        print(f"The error '{e}' occurred")
    return connection