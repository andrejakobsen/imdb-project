import os, uuid
from typing import Container
import yaml
from dotenv import find_dotenv, load_dotenv
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

load_dotenv(find_dotenv())
key = os.getenv('AZURE_KEY')
connection_string = os.getenv('CONNECTION_STRING')

def load_config():
    dir_root = os.path.dirname(os.path.abspath(__file__))
    with open(dir_root + '/config.yaml', 'r') as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.FullLoader)

def get_files(dir):
    with os.scandir(dir) as entries:
        for entry in entries:
            if entry.is_file() and not entry.name.startswith('.'):
                yield entry

def upload(files, connection_string, container_name):
    container_client = ContainerClient.from_connection_string(connection_string, container_name)
    for file in files:
        blob_client = container_client.get_blob_client(file.name)
        with open(file.path, 'rb') as data:
            blob_client.upload(data)
            print(f'{file.name} uploaded to blob storage')

config = load_config()
files = get_files(config['source_folder'])
print(*files)
# try:
#     print()
#     blob_service_client = BlobServiceClient.from_connection_string(connection_string)
#     container_name = str(uuid.uuid4())
#     container_client = blob_service_client.create_container(container_name)
# except Exception as e:
#     print(e)
