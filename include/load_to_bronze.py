import os
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient


load_dotenv()

credential = ClientSecretCredential(
    tenant_id=os.getenv("AZURE_TENANT_ID"),
    client_id=os.getenv("AZURE_CLIENT_ID"),
    client_secret=os.getenv("AZURE_CLIENT_SECRET")
)

account_url = "https://pokemondatacr.dfs.core.windows.net/"
client = DataLakeServiceClient(account_url=account_url, credential=credential)

client.create_file_system("bronze")

##pasta_bronze = client.get_file_system_client("gold")
##filesystem_client.delete_directory("diretorio_via_python")