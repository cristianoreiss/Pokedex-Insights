from datetime import datetime, timedelta

from airflow.sdk import Asset, dag, task
from extract_pokemon import extract_pokemon
##from plugins.upload_to_bronze import upload_to_bronze
from pendulum import datetime

@dag(
    start_date = datetime(2026, 4, 23,12),
    schedule="@daily",
    default_args={
        "owner": "Cristiano",
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    catchup=False
)
def pipeline_pokedex():

    @task
    def task_extracao():
        extract_pokemon()

    @task
    def task_load_bronze():
        pass

    task_extracao() >> task_load_bronze()

pipeline_pokedex()

