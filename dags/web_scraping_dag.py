from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scraper import scrape_all_pokemon_data
from cleaner import clean_all
from upload_to_snowflake import upload_to_snowflake

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG("pokemon_pipeline_dag",
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    task_scrape = PythonOperator(
        task_id="scrape_pokemon_data",
        python_callable=scrape_all_pokemon_data
    )

    task_clean = PythonOperator(
        task_id="clean_all_data",
        python_callable=clean_all
    )

    task_upload = PythonOperator(
        task_id="upload_to_snowflake",
        python_callable=upload_to_snowflake
    )

    task_scrape >> task_clean >> task_upload
