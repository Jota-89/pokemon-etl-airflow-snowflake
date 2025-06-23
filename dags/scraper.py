import requests
import pandas as pd
import time


def scrape_all_pokemon_data():
    total = 1302
    base_url = "https://pokeapi.co/api/v2/pokemon/"

    main_data = []
    type_data = []
    ability_data = []
    stat_data = []
    move_data = []

    for i in range(1, total + 1):
        print(f" Descargando Pokémon #{i}")
        response = requests.get(f"{base_url}{i}")
        if response.status_code != 200:
            print(f" Error al acceder al ID {i}")
            continue

        data = response.json()

        main_data.append({
            "ID": data['id'],
            "Name": data['name'].title(),
            "Height": data['height'],
            "Weight": data['weight'],
            "Base Experience": data['base_experience'],
            "Order": data['order'],
            "Sprite": data['sprites']['front_default']
        })

        for t in data['types']:
            type_data.append({
                "Pokemon ID": data['id'],
                "Type": t['type']['name']
            })

        for ab in data['abilities']:
            ability_data.append({
                "Pokemon ID": data['id'],
                "Ability": ab['ability']['name'],
                "Is Hidden": ab['is_hidden']
            })

        for st in data['stats']:
            stat_data.append({
                "Pokemon ID": data['id'],
                "Stat": st['stat']['name'],
                "Base Stat": st['base_stat'],
                "Effort": st['effort']
            })

        # Movimientos
        for mv in data['moves']:
            move_data.append({
                "Pokemon ID": data['id'],
                "Move": mv['move']['name']
            })

        time.sleep(0.1)  # Para no saturar la API

    # Guardar como CSV
    pd.DataFrame(main_data).to_csv(
        "/opt/airflow/dags/pokemon_main.csv", index=False)
    pd.DataFrame(type_data).to_csv(
        "/opt/airflow/dags/pokemon_types.csv", index=False)
    pd.DataFrame(ability_data).to_csv(
        "/opt/airflow/dags/pokemon_abilities.csv", index=False)
    pd.DataFrame(stat_data).to_csv(
        "/opt/airflow/dags/pokemon_stats.csv", index=False)
    pd.DataFrame(move_data).to_csv(
        "/opt/airflow/dags/pokemon_moves.csv", index=False)

    print("Todos los datos fueron guardados correctamente.")
