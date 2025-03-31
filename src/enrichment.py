import os
import sqlite3
import pandas as pd
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from bs4 import BeautifulSoup
import csv

# Configuración de rutas
DB_PATH = "src/static/db/ingestion.db"
CLEANED_DATA_PATH = "src/static/xlsx/cleaned_data.xlsx"
LANGUAGES_DATA_PATH = "src/Dataset2_Actividad3/languages_dataset.csv"
ENRICHED_DATA_PATH = "src/static/xlsx/enriched_data.xlsx"
ENRICHMENT_REPORT_PATH = "src/static/auditoria/enrichment_report.txt"

# Asegurar directorios
os.makedirs(os.path.dirname(ENRICHED_DATA_PATH), exist_ok=True)
os.makedirs(os.path.dirname(ENRICHMENT_REPORT_PATH), exist_ok=True)

def check_cleaned_data_exists():
    """Verifica si los datos limpios existen o ejecuta el script de procesamiento"""
    if not os.path.exists(CLEANED_DATA_PATH):
        print(f"Los datos limpios no existen en {CLEANED_DATA_PATH}. Ejecutando simulacion_procesamiento.py...")
        import simulacion_procesamiento
        simulacion_procesamiento.main()
        return True
    
    print(f"Datos limpios encontrados en {CLEANED_DATA_PATH}")
    return False

def load_cleaned_data():
    """Carga los datos limpios desde el archivo Excel"""
    print("\n=== CARGANDO DATOS LIMPIOS ===")
    try:
        cleaned_df = pd.read_excel(CLEANED_DATA_PATH)
        print(f"Datos cargados correctamente: {len(cleaned_df)} registros")
        return cleaned_df
    except Exception as e:
        print(f"Error al cargar los datos limpios: {e}")
        return None

def load_languages_data():
    """Carga los datos de idiomas desde múltiples fuentes"""
    print("\n=== CARGANDO DATOS DE FUENTES ADICIONALES ===")
    
    # 1. Cargar datos de idiomas desde CSV
    print(f"Cargando datos de idiomas desde {LANGUAGES_DATA_PATH}...")
    try:
        languages_df = pd.read_csv(LANGUAGES_DATA_PATH)
        print(f"  - CSV cargado correctamente: {len(languages_df)} registros")
        
        # Procesar y limpiar datos de idiomas
        languages_df.columns = languages_df.columns.str.lower().str.strip()
        
        # Eliminar duplicados si existen
        before_dedup = len(languages_df)
        languages_df.drop_duplicates(subset=['language', 'iso code'], inplace=True)
        after_dedup = len(languages_df)
        if before_dedup > after_dedup:
            print(f"  - Se eliminaron {before_dedup - after_dedup} registros duplicados del dataset de idiomas")
        
        # Convertir códigos ISO a minúsculas para facilitar la unión
        languages_df['iso code'] = languages_df['iso code'].str.lower()
        
        return languages_df
    except Exception as e:
        print(f"Error al cargar los datos de idiomas: {e}")
        return pd.DataFrame()  # Retornar DataFrame vacío en caso de error

def extract_country_languages(countries_df):
    """Extrae y estructura la información de idiomas de cada país"""
    print("\n=== PROCESANDO DATOS DE IDIOMAS DE PAÍSES ===")
    
    # Crear DataFrame para almacenar la relación país-idiomas
    country_languages = []
    
    # Iterar sobre cada país y extraer sus idiomas
    for _, row in countries_df.iterrows():
        cca3 = row['cca3']
        country_name = row['name_common']
        
        # Convertir el campo languages de JSON string a diccionario
        try:
            if pd.notna(row['languages']) and row['languages'] != '{}':
                languages_dict = json.loads(row['languages'])
                
                # Extraer cada idioma y su código
                for iso_code, language in languages_dict.items():
                    country_languages.append({
                        'cca3': cca3,
                        'country_name': country_name,
                        'iso_code': iso_code.lower(),  # Convertir a minúsculas para consistencia
                        'language_name': language
                    })
        except (json.JSONDecodeError, TypeError) as e:
            print(f"  - Error al procesar idiomas para {country_name}: {e}")
            continue
    
    # Crear DataFrame con la información extraída
    country_languages_df = pd.DataFrame(country_languages)
    
    print(f"Se extrajeron {len(country_languages_df)} relaciones país-idioma")
    return country_languages_df

def enrich_data(countries_df, country_languages_df, languages_df):
    """Enriquece los datos de países con información adicional de idiomas"""
    print("\n=== ENRIQUECIENDO DATOS ===")
    
    # 1. Crear una copia del DataFrame original para no modificarlo
    enriched_df = countries_df.copy()
    
    # 2. Agregar columnas para almacenar información enriquecida
    enriched_df['primary_language'] = None
    enriched_df['language_family'] = None
    enriched_df['writing_system'] = None
    enriched_df['language_count'] = 0
    enriched_df['languages_list'] = ""
    
    # Estadísticas para el reporte
    match_stats = {
        'countries_with_languages': 0,
        'countries_enriched': 0,
        'total_language_matches': 0,
        'countries_without_matches': []
    }
    
    # 3. Para cada país, buscar coincidencias en los datos de idiomas
    for i, country in enriched_df.iterrows():
        # Obtener los idiomas asociados a este país
        country_langs = country_languages_df[country_languages_df['cca3'] == country['cca3']]
        
        if not country_langs.empty:
            match_stats['countries_with_languages'] += 1
            language_matches = []
            country_enriched = False
            
            # Almacenar la cantidad de idiomas
            enriched_df.at[i, 'language_count'] = len(country_langs)
            
            # Crear lista de idiomas
            langs_list = ', '.join(country_langs['language_name'].tolist())
            enriched_df.at[i, 'languages_list'] = langs_list
            
            # Buscar coincidencias con el dataset de idiomas
            for _, lang in country_langs.iterrows():
                # Buscar por código ISO
                lang_match = languages_df[languages_df['iso code'] == lang['iso_code']]
                
                if not lang_match.empty:
                    language_matches.append(lang['language_name'])
                    match_stats['total_language_matches'] += 1
                    
                    # Si es el primer match, usar como idioma principal
                    if pd.isna(enriched_df.at[i, 'primary_language']):
                        enriched_df.at[i, 'primary_language'] = lang['language_name']
                        enriched_df.at[i, 'language_family'] = lang_match.iloc[0]['family']
                        enriched_df.at[i, 'writing_system'] = lang_match.iloc[0]['writing system']
                        country_enriched = True
            
            if country_enriched:
                match_stats['countries_enriched'] += 1
            else:
                match_stats['countries_without_matches'].append(country['name_common'])
    
    # 4. Calcular estadísticas adicionales
    print(f"  - Países con información de idiomas: {match_stats['countries_with_languages']}")
    print(f"  - Países enriquecidos con datos adicionales: {match_stats['countries_enriched']}")
    print(f"  - Total de coincidencias de idiomas: {match_stats['total_language_matches']}")
    
    return enriched_df, match_stats

def calculate_additional_metrics(enriched_df):
    """Calcula métricas adicionales basadas en los datos enriquecidos"""
    print("\n=== CALCULANDO MÉTRICAS ADICIONALES ===")
    
    # Crear copia para no modificar el DataFrame original
    final_df = enriched_df.copy()
    
    # 1. Calcular la densidad lingüística (idiomas por millón de habitantes )
    final_df['language_density'] = final_df.apply(
        lambda row: (row['language_count'] * 1000000 / row['population']) 
        if row['population'] > 0 and row['language_count'] > 0 else 0, 
        axis=1
    )
    
    # 2. Clasificar países por diversidad lingüística
    def classify_linguistic_diversity(row):
        if row['language_count'] == 0:
            return "No data"
        elif row['language_count'] == 1:
            return "Monolingual"
        elif row['language_count'] <= 3:
            return "Low diversity"
        elif row['language_count'] <= 10:
            return "Medium diversity"
        else:
            return "High diversity"
    
    final_df['linguistic_diversity'] = final_df.apply(classify_linguistic_diversity, axis=1)
    
    # 3. Identificar familias lingüísticas principales por región
    region_language_families = {}
    for region in final_df['region'].unique():
        if pd.isna(region):
            continue
            
        region_df = final_df[final_df['region'] == region]
        language_families = region_df['language_family'].value_counts().to_dict()
        region_language_families[region] = language_families
    
    print("  - Calculada densidad lingüística por millón de habitantes")
    print("  - Clasificados países por diversidad lingüística")
    print("  - Identificadas familias lingüísticas principales por región")
    
    return final_df, region_language_families

def generate_output_files(enriched_df, match_stats, region_language_families):
    """Genera los archivos de salida: datos enriquecidos y reporte de auditoría"""
    print("\n=== GENERANDO ARCHIVOS DE SALIDA ===")
    
    # 1. Exportar datos enriquecidos a Excel
    print(f"Exportando datos enriquecidos a {ENRICHED_DATA_PATH}...")
    enriched_df.to_excel(ENRICHED_DATA_PATH, index=False)
    
    # 2. Generar reporte de auditoría
    print(f"Generando reporte de auditoría en {ENRICHMENT_REPORT_PATH}...")
    
    with open(ENRICHMENT_REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write("INFORME DE AUDITORÍA DEL PROCESO DE ENRIQUECIMIENTO DE DATOS\n")
        f.write("=========================================================\n\n")
        f.write(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Resumen del proceso
        f.write("1. RESUMEN DEL PROCESO DE ENRIQUECIMIENTO\n")
        f.write("---------------------------------------\n")
        f.write(f"Total de países en el dataset base: {len(enriched_df)}\n")
        f.write(f"Países con información de idiomas: {match_stats['countries_with_languages']}\n")
        f.write(f"Países enriquecidos con datos adicionales: {match_stats['countries_enriched']}\n")
        f.write(f"Total de coincidencias de idiomas: {match_stats['total_language_matches']}\n\n")
        
        # Detalle de países sin coincidencias
        f.write("2. PAÍSES SIN COINCIDENCIAS EN EL DATASET DE IDIOMAS\n")
        f.write("------------------------------------------------\n")
        if match_stats['countries_without_matches']:
            for country in match_stats['countries_without_matches'][:20]:  # Limitar a 20 para no hacer el reporte demasiado largo
                f.write(f"  - {country}\n")
            
            if len(match_stats['countries_without_matches']) > 20:
                f.write(f"  ... y {len(match_stats['countries_without_matches']) - 20} países más\n")
        else:
            f.write("  No hay países sin coincidencias.\n")
        f.write("\n")
        
        # Estadísticas sobre familias lingüísticas por región
        f.write("3. FAMILIAS LINGÜÍSTICAS PRINCIPALES POR REGIÓN\n")
        f.write("-------------------------------------------\n")
        for region, families in region_language_families.items():
            if not families:
                continue
                
            f.write(f"\nRegión: {region}\n")
            sorted_families = sorted(families.items(), key=lambda x: x[1], reverse=True)
            
            for i, (family, count) in enumerate(sorted_families):
                if i >= 5:  # Limitar a las 5 familias más comunes
                    break
                if pd.notna(family):
                    f.write(f"  - {family}: {count} países\n")
        f.write("\n")
        
        # Métricas lingüísticas
        f.write("4. MÉTRICAS LINGÜÍSTICAS DEL DATASET ENRIQUECIDO\n")
        f.write("--------------------------------------------\n")
        diversity_counts = enriched_df['linguistic_diversity'].value_counts().to_dict()
        for diversity, count in diversity_counts.items():
            f.write(f"  - {diversity}: {count} países\n")
        
        # Promedio de idiomas por país
        avg_languages = enriched_df['language_count'].mean()
        f.write(f"\nPromedio de idiomas por país: {avg_languages:.2f}\n")
        
        # País con mayor diversidad lingüística
        if enriched_df['language_count'].max() > 0:
            most_diverse = enriched_df.loc[enriched_df['language_count'].idxmax()]
            f.write(f"País con mayor diversidad lingüística: {most_diverse['name_common']} "
                   f"({most_diverse['language_count']} idiomas)\n\n")
        
        # Muestra de los datos enriquecidos
        f.write("5. MUESTRA DE DATOS ENRIQUECIDOS\n")
        f.write("-----------------------------\n")
        sample_columns = ['cca3', 'name_common', 'region', 'primary_language', 
                         'language_family', 'language_count', 'linguistic_diversity']
        f.write(enriched_df.head(5)[sample_columns].to_string())
        
    print(f"Archivos generados exitosamente.")

def main():
    print("\n===== INICIANDO PROCESO DE ENRIQUECIMIENTO DE DATOS =====\n")
    
    # 1. Verificar si los datos limpios existen o ejecutar simulacion_procesamiento.py
    check_cleaned_data_exists()
    
    # 2. Cargar los datos limpios
    countries_df = load_cleaned_data()
    if countries_df is None:
        print("Error: No se pudieron cargar los datos limpios. Abortando proceso.")
        return
    
    # 3. Cargar datos adicionales de idiomas
    languages_df = load_languages_data()
    if languages_df.empty:
        print("Error: No se pudieron cargar los datos de idiomas. Abortando proceso.")
        return
    
    # 4. Extraer y estructurar información de idiomas de cada país
    country_languages_df = extract_country_languages(countries_df)
    
    # 5. Enriquecer los datos de países con información de idiomas
    enriched_df, match_stats = enrich_data(countries_df, country_languages_df, languages_df)
    
    # 6. Calcular métricas adicionales
    final_df, region_language_families = calculate_additional_metrics(enriched_df)
    
    # 7. Generar archivos de salida
    generate_output_files(final_df, match_stats, region_language_families)
    
    print("\n===== PROCESO DE ENRIQUECIMIENTO COMPLETADO =====")

if __name__ == "__main__":
    main()