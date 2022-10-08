import os, uuid
from typing import Container
import yaml
import click
from dotenv import find_dotenv, load_dotenv
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError
from constants import AZURE_CONNECTION_STRING
import pandas as pd

def upload_files_from_folder(folder: str, connection_string: str, container_name: str):
    """Upload all the files given in `folder` to the blob storage based
    on `container_name`.  

    Parameters
    ----------
    folder : str
        Source folder containing the files to be uploaded.
    connection_string : str
        Azure connection string for a storage account name to
        authenticate the user. The connection string can be found in
        Azure portal under `Access keys` for your storage account.
    container_name : str
        Name of a container associated with the storage account as given
        by the `connection_string`.
    """
    container_client = ContainerClient.from_connection_string(connection_string,
                                                              container_name)
    files = _get_files(folder)
    click.secho('\nStarting to upload files...', bg='green')
    for file in files:
        blob_client = container_client.get_blob_client(file.name)
        file_name = f'"{file.name}"'
        try:
            message = click.style(f'{file_name} successfully uploaded to blob storage.',
                                  fg='green')
            _upload_file(file, blob_client, message)
        except ResourceExistsError:
            click.secho(f'Blob {file_name} already exists.', fg='white', bg='red')
            if click.confirm('Do you wish to overwrite the file?',
                             default=True):
                message = click.style(f'{file_name} has been overwritten in blob storage.\n',
                                      fg='green')
                _upload_file(file, blob_client, message, overwrite=True)
            else: click.secho(f'{file_name} was not overwritten in blob storage.\n',
                              fg='yellow')
        except Exception as e:
            click.secho(f'\nSomething went wrong with uploading {file_name}...', fg='red')
            print(e)

def _load_config():
    dir_root = os.path.dirname(os.path.abspath(__file__))
    with open(dir_root + '/config.yaml', 'r') as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.FullLoader)

def _get_files(dir: str):
    with os.scandir(dir) as entries:
        for entry in entries:
            if entry.is_file() and not entry.name.startswith('.'):
                yield entry

def _upload_file(file, blob_client, message, overwrite=False):
    with open(file.path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=overwrite)
        click.echo(message)

if __name__ == '__main__':
    config = _load_config()
    upload_files_from_folder(folder=config['source_folder'],
                             connection_string=AZURE_CONNECTION_STRING,
                             container_name=config['container_name'])
