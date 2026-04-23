from datetime import datetime, timedelta

from airflow.sdk import Asset, dag, task
from include.extracao_pokemon import extracao_pokemon
from pendulum import datetime

@dag(
    start_date = datetime(2026, 4, 26,9),
    schedule="@daily",
    default_args={
        "owner": "Cristiano",
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    }
)
def pipeline_pokedex():

    @task
    def task_extracao():
        extracao_pokemon()

    @task
    def task_transformacao():
        pass


    task_extracao() >> task_transformacao()

pipeline_pokedex()

