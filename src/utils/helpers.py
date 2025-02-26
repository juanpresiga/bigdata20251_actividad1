import json
import os
import pandas as pd

def create_json_file(data, filename='request_api_json.json'):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    print(f"Archivo JSON '{filename}' generado exitosamente.")

def create_xlsx_file(df, filename='src/static/xlsx/request_api_xlsx.xlsx'):
    df.to_excel(filename, index=False)
    print(f"Archivo Excel '{filename}' generado exitosamente.")



def audit_data(df, audit_filename='src/static/audit/auditoria.txt'):

    num_rows = len(df)

    data_types = df.dtypes
    
    null_counts = df.isnull().sum()
    
    audit_dir = os.path.dirname(audit_filename)

    if not os.path.exists(audit_dir):
        os.makedirs(audit_dir)
    
    with open(audit_filename, 'w') as audit_file:
        audit_file.write(f"Número de filas: {num_rows}\n")
        audit_file.write("Tipos de datos de cada columna:\n")
        audit_file.write(f"{data_types}\n")
        audit_file.write("Número de valores nulos en cada columna:\n")
        audit_file.write(f"{null_counts}\n")
    
    print(f"Archivo de auditoría '{audit_filename}' generado exitosamente.")