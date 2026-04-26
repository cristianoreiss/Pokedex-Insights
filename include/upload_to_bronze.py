import os
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from pymongo import MongoClient
from bson.json_util import dumps

# Obter as variáveis de ambiente
load_dotenv()
conexao_mongodb = os.getenv("MONGO_URI")
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

# Função para obter a coleção do mongodb
def get_mongo_collection(conexao_mongodb,database,collection):
    client_mongodb = MongoClient(conexao_mongodb)
    db = client_mongodb[database]
    collection = db[collection]
    return collection

# Função de coneção ao Azure
def connect_azure(tenent_id,client_id,client_secrect):
    credential = ClientSecretCredential(
        tenant_id=tenent_id,
        client_id=client_id,
        client_secret=client_secrect
    )
    return credential

# Função para obter o aqruivo bronze_pokemon.json no Azure
def get_azure_file(account_url,credential,nome_conteiner,caminho_arquivo):
    client_azure = DataLakeServiceClient(
        account_url=account_url,
        credential=credential,
        connection_timeout = 600,
        read_timeout=600,
        retry_total=5
    )
    conteiner = client_azure.get_file_system_client(file_system=nome_conteiner)
    arquivo_azure = conteiner.get_file_client(caminho_arquivo)

    return arquivo_azure



# Função extrair os dados do mongodb
def extract_data_mongo(collection):
    lista_docs = []
    docs = collection.find()
    for doc in docs:
        doc["_id"] = str(doc["_id"])
        lista_docs.append(doc)
    lista_json = dumps(lista_docs)

    return lista_json


# Função para realizar o upload dos dados no adls gen2 (principal)
def upload_to_bronze():
    collection = get_mongo_collection(conexao_mongodb,"pokedex_db","raw_pokemon")
    credential = connect_azure(AZURE_TENANT_ID,AZURE_CLIENT_ID,AZURE_CLIENT_SECRET)
    account_url = "https://pokemondatacr.dfs.core.windows.net/"
    bronze_pokedex = get_azure_file(account_url,credential,"bronze","bronze_pokedex/bronze_pokedex.json")
    lista_json = extract_data_mongo(collection)
    try:
        print("Iniciando upload para o Azure")
        bronze_pokedex.upload_data(
            lista_json,
            overwrite=True,
            timeout=600,
            max_concurrency=1
        )
        print("Upload concluído")
    except Exception as e:
        print(f"Erro detectado: {e}")

if __name__ == "__main__":
    upload_to_bronze()
