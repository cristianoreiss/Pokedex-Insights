import os
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from pymongo import MongoClient
from bson.json_util import dumps


load_dotenv()
conexao_mongodb = os.getenv("MONGO_URI")
client_mongodb = MongoClient(conexao_mongodb)
db = client_mongodb["pokedex_db"]
collection = db['bronze_pokemon']


credential = ClientSecretCredential(
    tenant_id=os.getenv("AZURE_TENANT_ID"),
    client_id=os.getenv("AZURE_CLIENT_ID"),
    client_secret=os.getenv("AZURE_CLIENT_SECRET")
)

account_url = "https://pokemondatacr.dfs.core.windows.net/"
client_azure = DataLakeServiceClient(account_url=account_url, credential=credential)

docs = collection.find()
lista_docs = []

for doc in docs:
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
        lista_docs.append(doc)
lista_json = dumps(lista_docs)
print(type(lista_json))

container_bronze = client_azure.get_file_system_client(file_system="bronze")
bronze_pokedex = container_bronze.get_file_client("bronze_pokedex/bronze_pokedex.json")

bronze_pokedex.upload_data(lista_json,overwrite=True)
