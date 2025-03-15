import sqlite3
import pandas as pd
import numpy as np
import random
import string

DB_PATH = "src/static/db/ingestion.db"

def ensuciar_datos():
    print("\n=== ENSUCIANDO DATOS ORIGINALES (MODO SUAVE) ===")

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM countries", conn)
    conn.close()

    if df.empty:
        print("No hay datos para ensuciar.")
        return
    
    num_rows = len(df)

    # 🔵 1. Introducir valores nulos en un 1% de algunas columnas clave
    cols_with_nulls = ['subregion', 'capital']
    for col in cols_with_nulls:
        if col in df.columns:
            num_nulls = max(1, int(num_rows * 0.01))  # Garantiza al menos 1 afectado
            indices = np.random.choice(df.index, size=num_nulls, replace=False)
            df.loc[indices, col] = np.nan
    print("  - Se introdujeron algunos valores nulos en columnas seleccionadas.")

    # 🔵 2. Modificar valores numéricos ligeramente (±5% del valor original) en un 1%
    def distort_number(x):
        if pd.isnull(x) or not isinstance(x, (int, float)):
            return x
        factor = random.uniform(0.97, 1.03)
        return x * factor

    for col in ['population', 'area']:
        if col in df.columns:
            affected_indices = np.random.choice(df.index, size=max(1, int(num_rows * 0.01)), replace=False)
            df.loc[affected_indices, col] = df.loc[affected_indices, col].apply(distort_number)
    
    print("  - Se agregaron pequeñas variaciones en valores numéricos.")

    # 🔵 3. Duplicar solo el 0.5% de los registros
    if len(df) > 10:
        duplicated_rows = df.sample(n=max(1, int(num_rows * 0.005)), replace=True)
        df = pd.concat([df, duplicated_rows], ignore_index=True)
    print(f"  - Se duplicaron {len(duplicated_rows)} registros.")

    # 🔵 4. Introducir errores tipográficos en menos del 1% de los textos
    def introduce_typo(value):
        if isinstance(value, str) and len(value) > 3 and random.random() < 0.01:
            index = random.randint(0, len(value) - 2)
            return value[:index] + value[index + 1] + value[index] + value[index + 2:]
        return value

    text_cols = ['name_common', 'region']
    for col in text_cols:
        if col in df.columns:
            affected_indices = np.random.choice(df.index, size=max(1, int(num_rows * 0.01)), replace=False)
            df.loc[affected_indices, col] = df.loc[affected_indices, col].apply(introduce_typo)
    
    print("  - Se introdujeron errores tipográficos mínimos.")

    # 🔵 Guardar los datos ensuciados en la base de datos
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("countries", conn, if_exists="replace", index=False)
    conn.close()

    print("Datos ensuciados y guardados correctamente.")

if __name__ == "__main__":
    ensuciar_datos()
