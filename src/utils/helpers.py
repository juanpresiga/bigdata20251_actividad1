import json
import os
import io
import pandas as pd
from datetime import datetime


def create_file(data, filename, file_format='json'):
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    if file_format == 'json':
        with open(filename, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Archivo JSON '{filename}' generado exitosamente.")

    elif file_format == 'xlsx':
        if isinstance(data, pd.DataFrame):
            data.to_excel(filename, index=False)
            print(f"Archivo Excel '{filename}' generado exitosamente.")
        else:
            raise ValueError("Los datos deben ser un DataFrame para guardar como Excel.")

    elif file_format == 'csv':
        if isinstance(data, pd.DataFrame):
            data.to_csv(filename, index=False)
            print(f"Archivo CSV '{filename}' generado exitosamente.")
        else:
            raise ValueError("Los datos deben ser un DataFrame para guardar como CSV.")
    elif file_format == 'txt':
        if isinstance(data, pd.DataFrame):
            data.to_txt(filename, index=False)
            print(f"Archivo TXT '{filename}' generado exitosamente.")
        else:
            raise ValueError("Los datos deben ser un DataFrame para guardar como TXT.")

    else:
        raise ValueError(f"Formato de archivo no soportado: {file_format}")

def format_number(x):
    if pd.notnull(x):
        num = float(x)
        if num.is_integer():
            return '{:.0f}'.format(num)
        else:
            return '{:f}'.format(num).rstrip('0').rstrip('.')
    return x

def get_output_txt(df, method='info', rows=5):
    buffer = io.StringIO()
    
    if method == 'info':
        df.info(buf=buffer)
    elif method == 'head':
        print(df.head(rows), file=buffer)
    elif method == 'tail':
        print(df.tail(rows), file=buffer)
    elif method == 'describe':
        print(df.describe(), file=buffer)
    elif method == 'dtypes':
        print(df.dtypes, file=buffer)
    else:
        raise ValueError(f"Method {method} not supported")
        
    return buffer.getvalue()
def log_step(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_path = 'src/static/audit/cleaning_log.txt'
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] {message}\n")

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


def audit_data_md(df, audit_filename='src/static/audit/auditoria.md'):

    num_rows = len(df)

    data_types = df.dtypes
    
    null_counts = df.isnull().sum()
    
    audit_dir = os.path.dirname(audit_filename)

    if not os.path.exists(audit_dir):
        os.makedirs(audit_dir)

    def to_markdown_table(series, title):
        table = f"### {title}\n\n"
        table += "| Columna | Valor |\n"
        table += "|---------|-------|\n"
        for col, value in series.items():
            table += f"| {col} | {value} |\n"
        return table + "\n"

    with open(audit_filename, 'w') as audit_file:
        audit_file.write("# Auditoria del DataFrame\n\n")
        audit_file.write(f"## Numero de filas\n{num_rows}\n\n")
        audit_file.write(to_markdown_table(data_types, "Tipos de datos de cada columna"))
        audit_file.write(to_markdown_table(null_counts, "Numero de valores nulos en cada columna"))
    
    print(f"Archivo de auditoria '{audit_filename}' generado exitosamente.")