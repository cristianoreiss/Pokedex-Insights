from pymongo import MongoClient
from dotenv import load_dotenv
import os
import json

#carregar as variáveis de ambiente
load_dotenv()
CONNECTION_STRING = os.getenv("MONGO_URI")

client = MongoClient(CONNECTION_STRING)
db = client["pokedex_db"]
collection = db["raw_pokemon"]

documentos = collection.find()
lista_documentos = []
for doc in documentos:
    doc['_id'] = str(doc['_id'])
    lista_documentos.append(doc)

## converter para json
documentos_json = json.dumps(lista_documentos,indent=4, ensure_ascii=False)

print(documentos_json)