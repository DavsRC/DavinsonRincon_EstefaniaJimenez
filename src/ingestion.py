import os
import json
import sqlite3
import requests
import pandas as pd
from datetime import datetime

# Configuración
BASE_URL = "https://restcountries.com/v3.1/all"
DB_PATH = "src/static/db/ingestion.db"
EXCEL_PATH = "src/static/xlsx/ingestion.xlsx"
AUDIT_PATH = "src/static/auditoria/ingestion.txt"

# Asegurar directorios
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(os.path.dirname(EXCEL_PATH), exist_ok=True)
os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)

# Obtener datos de la API
def get_country_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error en la solicitud HTTP: {response.status_code}")
        return None

# Crear base de datos y tabla
def create_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='countries'")
    table_exists = cursor.fetchone()[0] == 1

    if table_exists:
        print("Base de datos existente. Eliminando datos antiguos...")
        cursor.execute('DROP TABLE IF EXISTS countries')
    else:
        print("Base de datos no existe. Creando nueva tabla...")

    cursor.execute('''
    CREATE TABLE countries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cca3 TEXT UNIQUE,
        name_common TEXT,
        name_official TEXT,
        region TEXT,
        subregion TEXT,
        population INTEGER,
        area REAL,
        languages TEXT,
        capital TEXT,
        timezones TEXT,
        currencies TEXT,
        flag TEXT,
        timestamp TEXT
    )
    ''')

    conn.commit()
    conn.close()

# Insertar datos de país
def insert_country_data(country_data):
    if not country_data:
        return False

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute('''
        INSERT INTO countries (
            cca3, name_common, name_official, region, subregion,
            population, area, languages, capital, timezones,
            currencies, flag, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            country_data.get('cca3'),
            country_data.get('name', {}).get('common'),
            country_data.get('name', {}).get('official'),
            country_data.get('region'),
            country_data.get('subregion'),
            country_data.get('population'),
            country_data.get('area'),
            json.dumps(country_data.get('languages', {})),
            json.dumps(country_data.get('capital', [])),
            json.dumps(country_data.get('timezones', [])),
            json.dumps(country_data.get('currencies', {})),
            country_data.get('flags', {}).get('png'),
            datetime.now().isoformat()
        ))
        conn.commit()
        conn.close()
        return True
    except sqlite3.Error as e:
        print(f"Error al insertar datos: {e}")
        conn.rollback()
        conn.close()
        return False

# Obtener datos de la base de datos
def get_db_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('''
    SELECT cca3, name_common, region, population, area 
    FROM countries
    ''')

    rows = cursor.fetchall()
    result = [dict(row) for row in rows]

    conn.close()
    return result

# Generar Excel
def generate_excel_sample():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT cca3, name_common, region, population, area FROM countries", conn)
    conn.close()
    df.to_excel(EXCEL_PATH, index=False)

# Generar archivo de auditoría
def generate_audit_file(api_data, db_data, was_reset):
    with open(AUDIT_PATH, 'w') as f:
        f.write("INFORME DE AUDITORÍA DE INGESTIÓN DE DATOS\n")
        f.write("=========================================\n\n")
        f.write(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write("1. RESUMEN DE INGESTIÓN\n")
        f.write("----------------------\n")
        if was_reset:
            f.write("Estado: La base de datos EXISTENTE fue eliminada y recreada.\n")
        else:
            f.write("Estado: La base de datos fue creada por primera vez.\n")
        f.write(f"Total de registros consultados en API: {len(api_data)}\n")
        f.write(f"Total de registros almacenados en BD: {len(db_data)}\n\n")

        f.write("2. COMPARACIÓN DETALLADA\n")
        f.write("------------------------\n")

        for country in api_data:
            country_name = country.get('name', {}).get('common')
            f.write(f"\nPaís: {country_name}\n")
            db_country = next((c for c in db_data if c['cca3'] == country.get('cca3')), None)
            if db_country:
                f.write("  - ESTADO: Almacenado correctamente en BD\n")
                if country.get('name', {}).get('common') == db_country['name_common']:
                    f.write("  - Nombre común: Coincide\n")
                if country.get('region') == db_country['region']:
                    f.write("  - Región: Coincide\n")
                if country.get('population') == db_country['population']:
                    f.write("  - Población: Coincide\n")

# Función principal
def main():
    print("Iniciando proceso de ingestión de datos...")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='countries'")
    was_reset = cursor.fetchone()[0] == 1
    conn.close()

    create_database()
    api_data = get_country_data()
    if api_data:
        for country in api_data:
            insert_country_data(country)

    db_data = get_db_data()
    generate_excel_sample()
    generate_audit_file(api_data, db_data, was_reset)

    if was_reset:
        print("Base de datos eliminada y recreada. Nuevos datos insertados.")
    else:
        print("Base de datos creada por primera vez. Datos insertados correctamente.")

    print("Proceso de ingestión completado.")

if __name__ == "__main__":
    main()
