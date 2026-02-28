from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import json

# 1 - Função de extração dos dados
def extracao_pokemon():
    pasta_atual = os.path.dirname(os.path.abspath(__file__))
    caminho_projeto = os.path.abspath(os.path.join(pasta_atual, '..','..'))
    caminho_pasta = os.path.join(caminho_projeto,"data","raw")
    caminho_arquivo = os.path.join(caminho_pasta, "bronze_pokemon.jsonl")


    url_api = "https://pokeapi.co/api/v2/pokemon?limit=1025"
    response = requests.get(url_api)


    poke_item = response.json()['results']


    with open(caminho_arquivo, "w") as f:
        for item in poke_item:
            nome_pokemon = item['name']
            url_pokemon = item['url']

            dict_pokemon = dict(
                nome=nome_pokemon,
                informacoes=requests.get(url_pokemon).json()
            )
            f.write(json.dumps(dict_pokemon, ensure_ascii=False) + "\n")

# 2 - Configurações da DAG
default_args = {
    'owner': 'cristiano',
    'start_date': datetime(2026,2,27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# 2 - Definição da DAG
with DAG(
    dag_id='bronze_pokemon',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id='extracao_api_pokemon',
        python_callable=extracao_pokemon,
    )



