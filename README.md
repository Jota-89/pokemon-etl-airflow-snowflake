# Proyecto ETL Pokémon con Airflow y Snowflake

Este proyecto consiste en la construcción de un pipeline ETL (Extracción, Transformación y Carga) utilizando Airflow para la orquestación de tareas y Snowflake como sistema de almacenamiento de datos. Todo se ejecuta en un entorno contenedorizado en Docker sobre Ubuntu.

## Objetivo

Demostrar de forma funcional cómo implementar un flujo automatizado que extrae información desde una API externa (PokeAPI), limpia y estandariza los datos, y los carga a una base de datos en Snowflake, dejándolos listos para análisis y consultas SQL complejas.

## Arquitectura

El pipeline consta de tres etapas principales:

### 1. Extracción de datos

Se consulta la API pública https://pokeapi.co/ para extraer:

- Identificador y nombre del Pokémon
- Altura, peso y experiencia base
- Tipos
- Habilidades
- Estadísticas base
- Movimientos

Los datos se organizan en cinco archivos `.csv`:
- `pokemon_main.csv`
- `pokemon_types.csv`
- `pokemon_abilities.csv`
- `pokemon_stats.csv`
- `pokemon_moves.csv`

### 2. Transformación

El script `cleaner.py` limpia y estandariza los datos:

- Elimina duplicados y registros nulos relevantes
- Estandariza nombres de columnas (mayúsculas, sin espacios)
- Asegura consistencia en tipos de datos
- Genera archivos limpios con el sufijo `_clean.csv`

### 3. Carga a Snowflake

El módulo `upload_to_snowflake.py`:

- Crea las tablas en Snowflake si no existen
- Carga los datos utilizando `write_pandas`
- Asegura que las columnas coincidan con el esquema en Snowflake

## Entorno

Definido mediante `docker-compose` con los servicios:

- PostgreSQL (para Airflow)
- Airflow Webserver
- Airflow Scheduler

Archivos clave:
- `docker-compose.yaml`
- `requirements.txt`
- `.gitignore`

## Instrucciones

1. Clonar el repositorio
2. Iniciar los servicios con Docker:

   ```bash
   docker-compose up --build

