import requests
import os
from pymongo import MongoClient, collection, ReplaceOne
from dotenv import load_dotenv

## Carregando as variáveis do .env
load_dotenv()
CONNECTION_STRING = os.getenv("MONGO_URI")
client = MongoClient(CONNECTION_STRING)

db = client['pokedex_db']
collection = db['bronze_pokemon']



def ingestao_pokemon():
    url_paginacao = "https://pokeapi.co/api/v2/pokemon/"

    while (url_paginacao):
        try:
            response = requests.get(url_paginacao)
            dados_brutos = response.json()
            lista_pokemons = dados_brutos['results']
            lista_para_input = []
            for pokemon in lista_pokemons:
                try:
                    detalhes_pokemon = requests.get(pokemon['url']).json()
                except Exception as f:
                    print(f"Erro ao pegar as informações: {f}")
                    continue
                operacao = ReplaceOne(
                    {"id": detalhes_pokemon["id"]},
                    detalhes_pokemon,
                    upsert=True
                )
                lista_para_input.append(operacao)
            if lista_para_input:
                collection.bulk_write(lista_para_input)
            url_paginacao = dados_brutos['next']
        except Exception as e:
            print(f"Ocorreu o seguinte erro: {e}")
            break
    print("Inserção feita!")

if __name__ == "__main__":
    ingestao_pokemon()

## 1 (OK) - Implementar o upsert do mongo para realizar a substituição
## 2 - Implementar o tratamento de erro para o caso de não encontrar algum registro
## 3 (OK) - Corrigir o while: "Python while True with conditional break": >> Tente inverter a ordem dentro do while. Em vez de buscar a "próxima página" no final do loop para usá-la na volta seguinte, tente estruturar de forma que o get da lista seja a primeira coisa a acontecer.
