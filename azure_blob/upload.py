import os, uuid
from typing import Container
import yaml
import click
from dotenv import find_dotenv, load_dotenv
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError
from constants import AZURE_CONNECTION_STRING


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
    container_client = ContainerClient.from_connection_string(connection_string,
                                                              container_name)
    print('\nStarting to upload files...\n')
    for file in files:
        blob_client = container_client.get_blob_client(file.name)
        file_name = f'"{file.name}"'
        try:
            message = click.style(f'{file_name} successfully uploaded to blob storage.',
                                  fg='green')
            _upload_blob(file, blob_client, message)
        except ResourceExistsError:
            click.secho(f'Blob {file_name} already exists.', fg='white', bg='red')
            if click.confirm('Do you wish to overwrite the file?',
                             default=True):
                message = click.style(f'{file_name} has been overwritten in blob storage.\n',
                                      fg='green')
                _upload_blob(file, blob_client, message, overwrite=True)
            else: click.secho(f'{file_name} was not overwritten in blob storage.\n',
                              fg='yellow')
        except Exception as e:
            click.secho(f'\nSomething went wrong with uploading {file_name}...', fg='red')
            print(e)

def _upload_blob(file, blob_client, message, overwrite=False):
    with open(file.path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=overwrite)
        click.echo(message)

config = load_config()
files = get_files(config['source_folder'])
upload(files, AZURE_CONNECTION_STRING, config['container_name'])
