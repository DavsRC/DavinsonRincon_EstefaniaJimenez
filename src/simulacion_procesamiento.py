import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import ingestion  # Importamos el módulo de ingestion.py
import ensuciar_datos  # Importar el nuevo módulo

# Configuración de rutas
DB_PATH = "src/static/db/ingestion.db"
CLEANED_DATA_PATH = "src/static/xlsx/cleaned_data.xlsx"
CLEANING_REPORT_PATH = "src/static/auditoria/cleaning_report.txt"

# Asegurar directorios
os.makedirs(os.path.dirname(CLEANED_DATA_PATH), exist_ok=True)
os.makedirs(os.path.dirname(CLEANING_REPORT_PATH), exist_ok=True)

def check_db_exists():
    """Verifica si la base de datos existe y contiene datos"""
    if not os.path.exists(DB_PATH):
        print(f"La base de datos no existe en {DB_PATH}. Ejecutando ingestion.py para crearla...")
        ingestion.main()
        return True
    
    # Verificar si hay datos en la tabla countries
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='countries'")
    table_exists = cursor.fetchone()[0] == 1
    
    if not table_exists:
        print("La tabla 'countries' no existe. Ejecutando ingestion.py para crearla...")
        conn.close()
        ingestion.main()
        return True
    
    cursor.execute("SELECT COUNT(*) FROM countries")
    count = cursor.fetchone()[0]
    conn.close()
    
    if count == 0:
        print("La tabla 'countries' está vacía. Ejecutando ingestion.py para poblarla...")
        ingestion.main()
        return True
    
    print(f"Base de datos encontrada con {count} registros.")
    return False

def load_data_from_db():
    """Carga los datos desde la base de datos a un DataFrame de Pandas"""
    conn = sqlite3.connect(DB_PATH)
    # Cargar todos los campos disponibles en la tabla countries
    query = """
    SELECT * FROM countries
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def exploratory_analysis(df):
    """Realiza un análisis exploratorio de los datos"""
    print("\n=== ANÁLISIS EXPLORATORIO ===")
    
    # Información básica del DataFrame
    print(f"Número total de registros: {len(df)}")
    
    # Verificar duplicados
    duplicates = df.duplicated().sum()
    print(f"Registros duplicados: {duplicates}")
    
    # Verificar valores nulos
    nulls = df.isnull().sum()
    print("\nValores nulos por columna:")
    print(nulls[nulls > 0] if not nulls.empty and nulls.max() > 0 else "No hay valores nulos")
    
    # Verificar tipos de datos
    print("\nTipos de datos:")
    print(df.dtypes)
    
    # Identificar posibles outliers en campos numéricos
    print("\nEstadísticas descriptivas para campos numéricos:")
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    if not numeric_cols.empty:
        print(df[numeric_cols].describe())
    
    return {
        "total_records": len(df),
        "duplicates": duplicates,
        "null_values": nulls.to_dict(),
        "outliers": {}  # Se puede implementar detección de outliers específica
    }

def clean_transform_data(df, analysis_results):
    """Limpia y transforma los datos según los problemas identificados"""
    print("\n=== LIMPIEZA Y TRANSFORMACIÓN DE DATOS ===")
    
    # Crear una copia para no modificar el DataFrame original
    cleaned_df = df.copy()
    
    # Estadísticas iniciales para el reporte
    initial_records = len(cleaned_df)
    
    # 1. Eliminación de duplicados
    print("Eliminando registros duplicados...")
    cleaned_df.drop_duplicates(inplace=True)
    duplicates_removed = initial_records - len(cleaned_df)
    print(f"  - {duplicates_removed} registros duplicados eliminados")
    
    # 2. Manejo de valores nulos
    print("Procesando valores nulos...")
    
    # Tratamiento específico por columna
    null_operations = {}
    
    # Columnas que no deberían tener nulos (claves primarias, identificadores)
    critical_cols = ['cca3', 'name_common', 'name_official']
    
    for col in cleaned_df.columns:
        null_count = cleaned_df[col].isnull().sum()
        
        if null_count > 0:
            if col in critical_cols:
                # Eliminar filas con valores nulos en columnas críticas
                before_drop = len(cleaned_df)
                cleaned_df.dropna(subset=[col], inplace=True)
                rows_dropped = before_drop - len(cleaned_df)
                null_operations[col] = f"Eliminadas {rows_dropped} filas con valores nulos"
            
            elif pd.api.types.is_numeric_dtype(cleaned_df[col]):
                # Rellenar valores numéricos con la mediana
                cleaned_df[col].fillna(cleaned_df[col].median(), inplace=True)
                null_operations[col] = f"Imputados {null_count} valores nulos con la mediana"
            
            else:
                # Rellenar otros tipos con cadena vacía o 'Unknown'
                cleaned_df[col].fillna("Unknown", inplace=True)
                null_operations[col] = f"Imputados {null_count} valores nulos con 'Unknown'"
    
    # 3. Corrección de tipos de datos
    print("Corrigiendo tipos de datos...")
    type_corrections = {}
    
    # Asegurar que population sea entero
    if 'population' in cleaned_df.columns:
        if cleaned_df['population'].dtype != 'int64':
            cleaned_df['population'] = cleaned_df['population'].astype('int64')
            type_corrections['population'] = 'Convertido a entero'
    
    # Asegurar que area sea float
    if 'area' in cleaned_df.columns:
        if cleaned_df['area'].dtype != 'float64':
            cleaned_df['area'] = cleaned_df['area'].astype('float64')
            type_corrections['area'] = 'Convertido a float'
    
    # 4. Transformaciones adicionales
    print("Aplicando transformaciones adicionales...")
    
    # Normalizar columnas de texto (convertir a minúsculas para consistencia)
    text_cols = cleaned_df.select_dtypes(include=['object']).columns
    text_transformations = {}
    
    for col in text_cols:
        # Solo normalizar columnas de texto simples, no JSON serializado
        if col not in ['languages', 'capital', 'timezones', 'currencies']:
            if isinstance(cleaned_df[col].iloc[0], str):
                cleaned_df[col] = cleaned_df[col].str.strip()
                text_transformations[col] = 'Eliminados espacios en blanco innecesarios'
    
    # Calcular densidad de población
    if 'population' in cleaned_df.columns and 'area' in cleaned_df.columns:
        # Evitar división por cero
        cleaned_df['population_density'] = cleaned_df.apply(
            lambda row: row['population'] / row['area'] if row['area'] > 0 else np.nan, 
            axis=1
        )
        print("  - Agregada columna 'population_density' (población/área)")
    
    # Resultado de la limpieza
    final_records = len(cleaned_df)
    
    return {
        'cleaned_df': cleaned_df,
        'stats': {
            'initial_records': initial_records,
            'final_records': final_records,
            'duplicates_removed': duplicates_removed,
            'null_operations': null_operations,
            'type_corrections': type_corrections,
            'text_transformations': text_transformations
        }
    }

def generate_output_files(cleaned_data, analysis_results, cleaning_results):
    """Genera los archivos de salida: datos limpios y reporte de auditoría"""
    print("\n=== GENERANDO ARCHIVOS DE SALIDA ===")
    
    # 1. Exportar datos limpios a Excel
    print(f"Exportando datos limpios a {CLEANED_DATA_PATH}...")
    cleaned_data.to_excel(CLEANED_DATA_PATH, index=False)
    
    # 2. Generar reporte de auditoría
    print(f"Generando reporte de auditoría en {CLEANING_REPORT_PATH}...")
    
    with open(CLEANING_REPORT_PATH, 'w') as f:
        f.write("INFORME DE AUDITORÍA DE LIMPIEZA DE DATOS\n")
        f.write("========================================\n\n")
        f.write(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Información del análisis exploratorio
        f.write("1. ANÁLISIS EXPLORATORIO INICIAL\n")
        f.write("-------------------------------\n")
        f.write(f"Total de registros analizados: {analysis_results['total_records']}\n")
        f.write(f"Registros duplicados encontrados: {analysis_results['duplicates']}\n")
        
        f.write("\nValores nulos por columna:\n")
        null_values = analysis_results['null_values']
        if any(null_values.values()):
            for col, count in null_values.items():
                if count > 0:
                    f.write(f"  - {col}: {count} valores nulos\n")
        else:
            f.write("  - No se encontraron valores nulos\n")
        
        # Información del proceso de limpieza
        f.write("\n2. PROCESO DE LIMPIEZA Y TRANSFORMACIÓN\n")
        f.write("-------------------------------------\n")
        stats = cleaning_results['stats']
        
        f.write(f"Registros antes de la limpieza: {stats['initial_records']}\n")
        f.write(f"Registros después de la limpieza: {stats['final_records']}\n")
        f.write(f"Registros eliminados: {stats['initial_records'] - stats['final_records']}\n\n")
        
        f.write("a. Eliminación de duplicados:\n")
        f.write(f"   - {stats['duplicates_removed']} registros duplicados eliminados\n\n")
        
        f.write("b. Manejo de valores nulos:\n")
        if stats['null_operations']:
            for col, operation in stats['null_operations'].items():
                f.write(f"   - {col}: {operation}\n")
        else:
            f.write("   - No fue necesario realizar operaciones con valores nulos\n\n")
        
        f.write("c. Corrección de tipos de datos:\n")
        if stats['type_corrections']:
            for col, correction in stats['type_corrections'].items():
                f.write(f"   - {col}: {correction}\n")
        else:
            f.write("   - No fue necesario realizar correcciones de tipos de datos\n\n")
        
        f.write("d. Transformaciones adicionales:\n")
        if stats['text_transformations']:
            for col, transformation in stats['text_transformations'].items():
                f.write(f"   - {col}: {transformation}\n")
        f.write("   - Agregada columna 'population_density' que representa la densidad de población\n\n")
        
        # Estadísticas finales
        f.write("\n3. ESTADÍSTICAS FINALES\n")
        f.write("----------------------\n")
        f.write(f"Total de registros en el conjunto de datos limpio: {stats['final_records']}\n")
        f.write(f"Columnas en el conjunto de datos limpio: {len(cleaned_data.columns)}\n")
        
        # Muestra de las primeras filas
        f.write("\nMuestra de los primeros 5 registros después de la limpieza:\n")
        f.write(cleaned_data.head(5)[['cca3', 'name_common', 'region', 'population', 'area']].to_string())
        
    print(f"Archivos generados exitosamente.")

def main():
    print("\n===== INICIANDO SIMULACIÓN DE PROCESAMIENTO DE DATOS =====\n")
    
    # 1. Verificar si la base de datos existe o ejecutar ingestion.py
    db_created = check_db_exists()

    # 2. Ensuciar los datos antes del análisis
    if not db_created:  # Solo ensuciar si la BD ya existía
        ensuciar_datos.ensuciar_datos()

    # 3. Cargar datos desde la base de datos
    df = load_data_from_db()
    print(f"Datos cargados desde la base de datos: {len(df)} registros")

    # 4. Realizar análisis exploratorio
    analysis_results = exploratory_analysis(df)

    # 5. Limpiar y transformar los datos
    cleaning_results = clean_transform_data(df, analysis_results)
    cleaned_df = cleaning_results['cleaned_df']

    # 6. Generar archivos de salida
    generate_output_files(cleaned_df, analysis_results, cleaning_results)

    print("\n===== PROCESO DE SIMULACIÓN COMPLETADO =====")

if __name__ == "__main__":
    main()