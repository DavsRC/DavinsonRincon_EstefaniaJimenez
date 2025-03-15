# Proyecto de Ingestión de Datos desde API (Proyecto Integrador - Big Data)

Este proyecto implementa un pipeline de ingestión de datos desde la API de Rest Countries hacia una base de datos SQLite, generando evidencias del proceso.

## Descripción de la solución

El proyecto extrae información sobre países desde la API de Rest Countries, almacena estos datos en una base de datos SQLite, y genera dos archivos de evidencia:

1. Un archivo Excel con una muestra de los registros almacenados
2. Un archivo de auditoría que compara los datos extraídos con los almacenados

Todo el proceso está automatizado mediante GitHub Actions para ejecutarse manualmente.

## Estructura del proyecto

```
nombre_apellido
├── setup.py            
├── README.md           
├── requirements.txt    
├── .github/workflows   
└── src                 
    ├── static          
    │   ├── auditoria   
    │   ├── db          
    │   └── xlsx        
    ├── ingestion.py    
    ├── simulacion_procesamiento.py
    └── ensuciar_datos.py

```

## Instrucciones de uso

### Requisitos previos

- Python 3.8 o superior
- Git

### Instalación local

1. Clonar el repositorio:

```bash
git clone https://github.com/DavsRC/DavinsonRincon_EstefaniaJimenez.git
cd DavinsonRincon_EstefaniaJimenez
```

2. crear entorno virtual

```bash
python -m venv venv
```

3. Activar el entorno virtual

```bash
venv\Scripts\activate
```

4. Instalar dependencias:

```bash
pip install -r requirements.txt
```

5. Ejecutar el script de ingestión:

```bash
python src/ingestion.py
```

6. Ejecutar el script de Preprocesamiento y Limpieza de Datos

   ```bash
python src/simulacion_procesamiento.py
```

## Automatización con GitHub Actions

El proyecto utiliza GitHub Actions para ejecutar automáticamente el proceso de ingestión de datos ".github/workflows/main.yml". El workflow está configurado para:

1. Crea entorno virtual
2. Activa el entorno virtual
3. Instalar todas las dependencias requeridas
4. Ejecuta el script de ingestion
5. Ejecutar el script de Preprocesamiento y Limpieza de Datos
6. Hace commit y push de los campos

### Verificación de la ejecución

Puedes verificar la ejecución exitosa del workflow en la pestaña "Actions" del repositorio. Después de cada ejecución, se generan:

- La base de datos SQLite en `src/static/db/ingestion.db`
- Un archivo Excel con una muestra de registros en `src/static/xlsx/ingestion.xlsx`
- Un archivo de auditoría en `src/static/auditoria/ingestion.txt`
- Un archivo de texto con el reporte del procesamiento y limpieza de los datos `src/static/auditoria/cleaning_report.txt`
- Un archivo de Excel con la data filtrada en `src/static/auditoria/cleaned_data.xlsx`


Estos archivos se actualizan en el repositorio después de cada ejecución exitosa del workflow.

## API utilizada

Este proyecto utiliza la API de Rest Countries (https://restcountries.com/v3.1/all), que proporciona información detallada sobre todos los países del mundo. Se utiliza el endpoint público `v3.1/all` para obtener datos completos de todos los países, incluyendo códigos, nombres, regiones, población, área, idiomas, capitales, zonas horarias, monedas y banderas.

## Datos almacenados

La base de datos SQLite almacena la siguiente información para cada país:

- Código ISO 3166-1 alpha-3 (cca3)
- Nombre común y oficial
- Región y subregión
- Población y área
- Idiomas
- Capital(es)
- Zonas horarias
- Monedas
- URL de la bandera
- Timestamp de la ingestión
