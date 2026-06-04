import os
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET=os.getenv("CLIENT_SECRET")

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

API_URL = "https://opensky-network.org/api/states/all"

def get_access_token():

    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    response = requests.post(TOKEN_URL, data=data)
    response.raise_for_status()

    return response.json()["access_token"]

def fetch_flights(token):
    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(API_URL, headers=headers, timeout=30)
    response.raise_for_status()

    return response.json()  