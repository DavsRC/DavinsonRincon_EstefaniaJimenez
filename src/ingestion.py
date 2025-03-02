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

def ensure_directories_exist():
    """Asegura que los directorios necesarios existan."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(EXCEL_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)

def get_country_data():
    """Obtiene datos de todos los países desde la API de restcountries.com."""
    response = requests.get(BASE_URL)
    
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error en la solicitud HTTP: {response.status_code}")
        return None

def create_database():
    """Crea la base de datos SQLite y la tabla de países si no existen."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Crear tabla de países
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS countries (
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

def insert_country_data(country_data):
    """Inserta los datos de un país en la base de datos."""
    if not country_data:
        return False
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Comprobar si el país ya existe
    cursor.execute("SELECT cca3 FROM countries WHERE cca3 = ?", (country_data.get('cca3'),))
    if cursor.fetchone():
        print(f"El país {country_data.get('name', {}).get('common')} ya existe en la base de datos.")
        conn.close()
        return False
    
    # Insertar país
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

def generate_excel_sample():
    """Genera un archivo Excel con una muestra de los datos almacenados."""
    conn = sqlite3.connect(DB_PATH)
    
    # Obtener todos los registros de la tabla countries
    query = "SELECT cca3, name_common, region, population, area FROM countries"
    df = pd.read_sql_query(query, conn)
    
    conn.close()
    
    # Guardar como Excel
    df.to_excel(EXCEL_PATH, index=False)
    print(f"Archivo Excel generado en {EXCEL_PATH}")

def generate_audit_file(api_data, db_data):
    """Genera un archivo de auditoría comparando datos de API vs. base de datos."""
    with open(AUDIT_PATH, 'w') as f:
        f.write("INFORME DE AUDITORÍA DE INGESTIÓN DE DATOS\n")
        f.write("=========================================\n\n")
        f.write(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("1. RESUMEN DE INGESTIÓN\n")
        f.write("----------------------\n")
        f.write(f"Total de registros consultados en API: {len(api_data)}\n")
        f.write(f"Total de registros encontrados en API: {sum(1 for d in api_data if d)}\n")
        f.write(f"Total de registros almacenados en BD: {len(db_data)}\n\n")
        
        # Comparación detallada
        f.write("2. COMPARACIÓN DETALLADA\n")
        f.write("------------------------\n")
        
        for country in api_data:
            country_name = country.get('name', {}).get('common')
            f.write(f"\nPaís: {country_name}\n")
            
            if not country:
                f.write("  - No encontrado en API\n")
                continue
            
            # Buscar en datos de BD
            db_country = next((c for c in db_data if c['cca3'] == country.get('cca3')), None)
            
            if db_country:
                f.write("  - ESTADO: Almacenado correctamente en BD\n")
                
                # Verificar algunos campos clave
                if country.get('name', {}).get('common') == db_country['name_common']:
                    f.write("  - Nombre común: Coincide\n")
                else:
                    f.write(f"  - Nombre común: Discrepancia - API: {country.get('name', {}).get('common')} vs BD: {db_country['name_common']}\n")
                
                if country.get('region') == db_country['region']:
                    f.write("  - Región: Coincide\n")
                else:
                    f.write(f"  - Región: Discrepancia - API: {country.get('region')} vs BD: {db_country['region']}\n")
                
                if country.get('population') == db_country['population']:
                    f.write("  - Población: Coincide\n")
                else:
                    f.write(f"  - Población: Discrepancia - API: {country.get('population')} vs BD: {db_country['population']}\n")
            else:
                f.write("  - ESTADO: No encontrado en BD\n")
        
        f.write("\n3. CONCLUSIÓN\n")
        f.write("-------------\n")
        if len(api_data) == len(db_data):
            f.write("La ingestión se completó correctamente. Todos los registros fueron almacenados.\n")
        else:
            f.write("La ingestión presenta discrepancias en el número de registros.\n")

def get_db_data():
    """Obtiene todos los datos almacenados en la base de datos."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT cca3, name_common, region, population, area 
    FROM countries
    ''')
    
    rows = cursor.fetchall()
    
    # Convertir a lista de diccionarios
    result = [dict(row) for row in rows]
    
    conn.close()
    return result

def main():
    """Función principal del script de ingestión."""
    print("Iniciando proceso de ingestión de datos...")
    
    # Asegurar que los directorios existan
    ensure_directories_exist()
    
    # Crear base de datos
    create_database()
    
    # Obtener datos de API y almacenarlos
    api_data = get_country_data()
    if api_data:
        for country in api_data:
            success = insert_country_data(country)
            if success:
                print(f"Datos de {country.get('name', {}).get('common')} insertados correctamente")
            else:
                print(f"No se pudieron insertar datos de {country.get('name', {}).get('common')}")
    
    # Obtener datos de la BD para auditoría
    db_data = get_db_data()
    
    # Generar archivo Excel
    generate_excel_sample()
    
    # Generar archivo de auditoría
    generate_audit_file(api_data, db_data)
    
    print("Proceso de ingestión completado")

if __name__ == "__main__":
    main()