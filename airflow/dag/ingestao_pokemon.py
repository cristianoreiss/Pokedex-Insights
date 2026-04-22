import requests
import os
from pymongo import MongoClient, collection
from dotenv import load_dotenv

## Carregando as variáveis do .env
load_dotenv()
CONNECTION_STRING = os.getenv("MONGO_URI")
client = MongoClient(CONNECTION_STRING)

db = client['pokedex_db']
collection = db['bronze_pokemon']



def ingestao_pokemon():
    url_paginacao = "https://pokeapi.co/api/v2/pokemon/"
    response = requests.get(url_paginacao)
    dados_brutos = response.json()
    url_detalhes_pokemon = url_paginacao

    while (url_detalhes_pokemon):
        lista_pokemons = dados_brutos['results']
        lista_para_input = []
        for pokemon in lista_pokemons:
            detalhes_pokemon = requests.get(pokemon['url']).json()
            lista_para_input.append(detalhes_pokemon)
        collection.insert_many(lista_para_input)
        url_detalhes_pokemon = dados_brutos['next']
        response = requests.get(url_detalhes_pokemon)
        dados_brutos = response.json()


if __name__ == "__main__":
    ingestao_pokemon()

## 1 - Implementar o upsert do mongo para realizar a substituição
## 2 - Implementar o tratamento de erro para o caso de não encontrar algum registro
## 3 - Corrigir o while: "Python while True with conditional break": >> Tente inverter a ordem dentro do while. Em vez de buscar a "próxima página" no final do loop para usá-la na volta seguinte, tente estruturar de forma que o get da lista seja a primeira coisa a acontecer.
