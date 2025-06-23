import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def upload_to_snowflake():
    conn = snowflake.connector.connect(
        user='USERNAME',
        password='YOURPASSWORD',
        account='YOURACCOUNT.snowflakecomputing.com',
        warehouse='MY_WH',
        database='POKEMON_DB',
        schema='PUBLIC'
    )

    tables = {
        "POKEMON_MAIN": {
            "path": "/opt/airflow/dags/pokemon_main_clean.csv",
            "schema": """
                CREATE TABLE IF NOT EXISTS POKEMON_MAIN (
                    "ID" INT,
                    "NAME" STRING,
                    "HEIGHT" INT,
                    "WEIGHT" INT,
                    "BASE_EXPERIENCE" INT
                );
            """
        },
        "POKEMON_TYPES": {
            "path": "/opt/airflow/dags/pokemon_types_clean.csv",
            "schema": """
                CREATE TABLE IF NOT EXISTS POKEMON_TYPES (
                    "POKEMON_ID" INT,
                    "TYPE" STRING
                );
            """
        },
        "POKEMON_ABILITIES": {
            "path": "/opt/airflow/dags/pokemon_abilities_clean.csv",
            "schema": """
                CREATE TABLE IF NOT EXISTS POKEMON_ABILITIES (
                    "POKEMON_ID" INT,
                    "ABILITY" STRING,
                    "IS_HIDDEN" BOOLEAN
                );
            """
        },
        "POKEMON_STATS": {
            "path": "/opt/airflow/dags/pokemon_stats_clean.csv",
            "schema": """
                CREATE TABLE IF NOT EXISTS POKEMON_STATS (
                    "POKEMON_ID" INT,
                    "STAT" STRING,
                    "BASE_STAT" INT,
                    "EFFORT" INT
                );
            """
        },
        "POKEMON_MOVES": {
            "path": "/opt/airflow/dags/pokemon_moves_clean.csv",
            "schema": """
                CREATE TABLE IF NOT EXISTS POKEMON_MOVES (
                    "POKEMON_ID" INT,
                    "MOVE" STRING
                );
            """
        }
    }

    for table_name, config in tables.items():
        print(f"Subiendo {table_name}...")

        df = pd.read_csv(config["path"])

        # Aseguramos mayúsculas y sin espacios
        df.columns = [col.upper().replace(" ", "_") for col in df.columns]

        # Creamos la tabla con identificadores entre comillas
        conn.cursor().execute(config["schema"])

        # Subimos los datos
        success, nchunks, nrows, _ = write_pandas(
            conn, df, table_name, quote_identifiers=True
        )
        print(f"{nrows} filas subidas correctamente a {table_name}.")

    conn.close()
