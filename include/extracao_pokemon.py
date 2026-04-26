import requests
import os
from pymongo import MongoClient, collection, ReplaceOne
from dotenv import load_dotenv

## Carregando as variáveis do .env
load_dotenv()
CONNECTION_STRING = os.getenv("MONGO_URI")

def pegar_colecao_banco(connection_string,db_name,collection_name):
    client = MongoClient(CONNECTION_STRING)
    db = client[db_name]
    collection = db[collection_name]
    return collection

def obter_detalhes_pokemon(sessao,url):
    response = sessao.get(url)
    detalhe_pokemon = response.json()
    return detalhe_pokemon

def preparar_lote_pokemon(lista_pokemon):
    lista_para_input = []
    for pokemon in lista_pokemon:
        operacao = ReplaceOne(
            {"id": pokemon["id"]},
            pokemon,
            upsert=True
        )
        lista_para_input.append(operacao)
    return lista_para_input


def extracao_pokemon():
    url_paginacao = "https://pokeapi.co/api/v2/pokemon/"
    sessao = requests.Session()
    colecao = pegar_colecao_banco(CONNECTION_STRING,"pokedex_db","raw_pokemon")

    while(url_paginacao):
        try:
            response = sessao.get(url_paginacao)
            pagina = response.json()
            dados_brutos = pagina['results']
            lista_pokemon = []
            for item in dados_brutos:
                url_pokemon = item["url"]
                try:
                    detalhe_pokemon = obter_detalhes_pokemon(sessao,url_pokemon)
                except Exception as e:
                    print(f"Erro: {e}")
                    continue
                lista_pokemon.append(detalhe_pokemon)
            lista_para_input = preparar_lote_pokemon(lista_pokemon)
            if lista_para_input:
                colecao.bulk_write(lista_para_input)
            url_paginacao = pagina['next']
        except Exception as e:
            print(f"erro: {e}")
            break
    print("Ingestão de dados feita!")

if __name__ == "__main__":
    extracao_pokemon()
