import os, uuid
from dotenv import find_dotenv, load_dotenv
load_dotenv(find_dotenv())

AZURE_CONNECTION_STRING = os.getenv('AZURE_CONNECTION_STRING')