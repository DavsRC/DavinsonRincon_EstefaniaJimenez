# Proyecto de Ingestión de Datos desde API (Proyecto Integrador - Big Data)

Este proyecto implementa un pipeline de ingestión de datos desde la API de Rest Countries hacia una base de datos SQLite, generando evidencias del proceso.

## Descripción de la solución

El proyecto extrae información sobre países desde la API de Rest Countries, almacena estos datos en una base de datos SQLite, y genera dos archivos de evidencia:

1. Un archivo Excel con una muestra de los registros almacenados
2. Un archivo de auditoría que compara los datos extraídos con los almacenados

Todo el proceso está automatizado mediante GitHub Actions para ejecutarse periódicamente.

## Estructura del proyecto

```
nombre_apellido
├── setup.py            # Configuración del paquete
├── README.md           # Este archivo
├── requirements.txt    # Dependencias del proyecto
├── .github/workflows   # Configuración de GitHub Actions
└── src                 # Código fuente
    ├── static          # Archivos generados
    │   ├── auditoria   # Archivos de auditoría
    │   ├── db          # Base de datos SQLite
    │   └── xlsx        # Archivos Excel generados
    └── ingestion.py    # Script principal de ingestión
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

2. Instalar dependencias:

```bash
pip install -r requirements.txt
```

3. Ejecutar el script de ingestión:

```bash
python src/ingestion.py
```

## Automatización con GitHub Actions

El proyecto utiliza GitHub Actions para ejecutar automáticamente el proceso de ingestión de datos. El workflow está configurado para:

1. Ejecutarse diariamente a medianoche
2. Ejecutarse manualmente cuando sea necesario
3. Instalar todas las dependencias requeridas
4. Ejecutar el script de ingestión
5. Verificar que todos los archivos se generan correctamente

### Verificación de la ejecución

Puedes verificar la ejecución exitosa del workflow en la pestaña "Actions" del repositorio. Después de cada ejecución, se generan:

- La base de datos SQLite en `src/static/db/ingestion.db`
- Un archivo Excel con una muestra de registros en `src/static/xlsx/ingestion.xlsx`
- Un archivo de auditoría en `src/static/auditoria/ingestion.txt`

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
