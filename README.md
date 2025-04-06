
# Proyecto Integrador de Big Data: Ingesta, Preprocesamiento y Enriquecimiento de Datos

Este proyecto implementa un **pipeline completo de procesamiento de datos** que simula un entorno de Big Data en la nube. Inicia con la **ingesta desde la API de Rest Countries**, pasa por una fase de **preprocesamiento y limpieza de datos**, seguida de un **enriquecimiento** con datos externos, y finaliza con el **almacenamiento y generación de evidencias** en una base de datos analítica (`SQLite`).

## Descripción de la Solución

El flujo general del proyecto incluye:

1. **Ingesta de datos** desde la API pública de Rest Countries.
2. **Preprocesamiento**: simulación de datos sucios, limpieza y filtrado de la información.
3. **Enriquecimiento**: integración de datos adicionales desde archivos complementarios (por ejemplo, idiomas).
4. **Almacenamiento** de los datos procesados en una base de datos `SQLite`.
5. **Generación de evidencias** (Excel, archivos de texto, auditorías).
6. **Automatización del pipeline** mediante GitHub Actions.

Todo el proceso simula una arquitectura cloud de procesamiento por lotes, con evidencias almacenadas como si fueran outputs de un flujo en producción.

## Estructura del Proyecto

```
nombre_apellido
├── setup.py
├── README.md
├── requirements.txt
├── .github/workflows
│   └── main.yml
└── src
    ├── static
    │   ├── auditoria
    │   ├── db
    │   └── xlsx
    ├── ingestion.py
    ├── simulacion_procesamiento.py
    ├── enrichment.py
    └── ensuciar_datos.py
```

## Instrucciones de Uso

### Requisitos

- Python 3.8 o superior
- Git

### Instalación local

```bash
# 1. Clonar el repositorio
git clone https://github.com/DavsRC/DavinsonRincon_EstefaniaJimenez.git
cd DavinsonRincon_EstefaniaJimenez

# 2. Crear y activar entorno virtual
python -m venv venv
venv\Scripts\activate  # En Windows

# 3. Instalar dependencias
pip install -r requirements.txt
```

### Ejecución de scripts

```bash
# 1. Ingesta de datos desde API
python src/ingestion.py

# 2. Preprocesamiento y limpieza de datos
python src/simulacion_procesamiento.py

# 3. Enriquecimiento de datos
python src/enrichment.py
```

## Automatización con GitHub Actions

El flujo completo está automatizado usando GitHub Actions en `.github/workflows/main.yml`. El pipeline realiza:

1. Creación del entorno virtual
2. Instalación de dependencias
3. Ejecución secuencial de scripts (`ingestion.py`, `simulacion_procesamiento.py`, `enrichment.py`)
4. Commit y push de los archivos generados como evidencia

### Archivos generados

Tras una ejecución exitosa se generan:

- `src/static/db/ingestion.db`: Base de datos SQLite con los datos finales
- `src/static/xlsx/ingestion.xlsx`: Muestra de los datos extraídos
- `src/static/auditoria/ingestion.txt`: Auditoría de la ingesta
- `src/static/auditoria/cleaning_report.txt`: Reporte del preprocesamiento
- `src/static/auditoria/cleaned_data.xlsx`: Datos filtrados
- `src/static/auditoria/enriched_data.xlsx`: Dataset final enriquecido
- `src/static/auditoria/enriched_report.txt`: Descripción del enriquecimiento

## API Utilizada

Se utiliza el endpoint público `https://restcountries.com/v3.1/all` de **Rest Countries API**, que entrega datos completos por país: nombre, región, idiomas, monedas, población, bandera, etc.

## Modelo de Datos

La base de datos `SQLite` resultante contiene una tabla principal con los siguientes campos:

- `cca3`: Código de país (clave primaria)
- `common_name`, `official_name`: Nombres del país
- `region`, `subregion`: Ubicación geopolítica
- `population`, `area`: Demográficos
- `languages`: Idiomas (como texto enriquecido)
- `capital`, `timezones`, `currencies`, `flag_url`: Atributos adicionales
- `timestamp`: Fecha/hora de la ingesta
- `language_family`, `language_complexity`: Datos agregados del enriquecimiento

## Beneficios y Simulación de Entorno Cloud

Este proyecto simula un entorno cloud al implementar:

- Automatización vía CI/CD con GitHub Actions (similar a pipelines en la nube)
- Uso de SQLite como base analítica local
- Separación de etapas del pipeline (como en un entorno distribuido)
- Generación de archivos y artefactos reproducibles

## Créditos

Proyecto desarrollado por:  
**Davinson Rincón & Estefanía Jiménez**  
Para la Actividad 3 del Proyecto Integrador - Big Data
