import pandas as pd


def clean_pokemon_data():
    input_path = "/opt/airflow/dags/pokemon_main.csv"
    output_path = "/opt/airflow/dags/pokemon_main_clean.csv"

    df = pd.read_csv(input_path)

    # Estandarizar columnas a mayúsculas y eliminar espacios
    df.columns = [col.strip().upper().replace(" ", "_") for col in df.columns]

    # Renombrar si es necesario para alinear con Snowflake
    df.rename(columns={
        "POKEMON_NAME": "NAME"
    }, inplace=True)

    df.drop(columns=["SPRITE", "ORDER"], errors='ignore', inplace=True)

    df.drop_duplicates(subset=["ID", "NAME"], inplace=True)
    df.dropna(subset=["ID", "NAME", "HEIGHT", "WEIGHT",
              "BASE_EXPERIENCE"], inplace=True)

    df["ID"] = df["ID"].astype(int)
    df["HEIGHT"] = df["HEIGHT"].astype(int)
    df["WEIGHT"] = df["WEIGHT"].astype(int)
    df["BASE_EXPERIENCE"] = df["BASE_EXPERIENCE"].astype(int)

    df.to_csv(output_path, index=False)
    print(
        f" Limpieza completada: {len(df)} Pokémon guardados en {output_path}")


def clean_pokemon_types():
    df = pd.read_csv("/opt/airflow/dags/pokemon_types.csv")
    df.columns = [col.strip().upper().replace(" ", "_") for col in df.columns]
    df.dropna(inplace=True)
    df["POKEMON_ID"] = df["POKEMON_ID"].astype(int)
    df["TYPE"] = df["TYPE"].astype(str)
    df.to_csv("/opt/airflow/dags/pokemon_types_clean.csv", index=False)
    print(f"Tipos limpiados: {len(df)} filas")


def clean_pokemon_abilities():
    df = pd.read_csv("/opt/airflow/dags/pokemon_abilities.csv")
    df.columns = [col.strip().upper().replace(" ", "_") for col in df.columns]
    df.dropna(inplace=True)
    df["POKEMON_ID"] = df["POKEMON_ID"].astype(int)
    df["ABILITY"] = df["ABILITY"].astype(str)
    df["IS_HIDDEN"] = df["IS_HIDDEN"].astype(bool)
    df.to_csv("/opt/airflow/dags/pokemon_abilities_clean.csv", index=False)
    print(f"Habilidades limpiadas: {len(df)} filas")


def clean_pokemon_stats():
    df = pd.read_csv("/opt/airflow/dags/pokemon_stats.csv")
    df.columns = [col.strip().upper().replace(" ", "_") for col in df.columns]
    df.dropna(inplace=True)
    df["POKEMON_ID"] = df["POKEMON_ID"].astype(int)
    df["STAT"] = df["STAT"].astype(str)
    df["BASE_STAT"] = df["BASE_STAT"].astype(int)
    df["EFFORT"] = df["EFFORT"].astype(int)
    df.to_csv("/opt/airflow/dags/pokemon_stats_clean.csv", index=False)
    print(f"Estadísticas limpiadas: {len(df)} filas")


def clean_pokemon_moves():
    df = pd.read_csv("/opt/airflow/dags/pokemon_moves.csv")
    df.columns = [col.strip().upper().replace(" ", "_") for col in df.columns]
    df.dropna(inplace=True)
    df["POKEMON_ID"] = df["POKEMON_ID"].astype(int)
    df["MOVE"] = df["MOVE"].astype(str)
    df.to_csv("/opt/airflow/dags/pokemon_moves_clean.csv", index=False)
    print(f"Movimientos limpiados: {len(df)} filas")


def clean_all():
    clean_pokemon_data()
    clean_pokemon_types()
    clean_pokemon_abilities()
    clean_pokemon_stats()
    clean_pokemon_moves()


if __name__ == "__main__":
    clean_all()
