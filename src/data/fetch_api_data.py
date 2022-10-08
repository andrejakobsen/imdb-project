import requests
import os
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())
API_KEY = os.environ.get('API_KEY')

url = "https://free-nba.p.rapidapi.com/games"
headers = {"X-RapidAPI-Key": API_KEY,
           "X-RapidAPI-Host": "free-nba.p.rapidapi.com"}
params = {"page": "0",
          "per_page": "1000"}

response = requests.get(url, headers=headers, params=params)

print(response.content)
