from kafka import KafkaProducer
from opensky_client import get_access_token, fetch_flights
from dotenv import load_dotenv
import requests

import json
import time
import os

dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("🚀 Flight Producer Started")

token = get_access_token()

while True:

    try:
        data = fetch_flights(token)

        recorded_time = data.get("time")
        states = data.get("states", [])

        flight_count = 0

        for flight in states:

            if flight[5] is None or flight[6] is None:
                continue

            payload = {
                "event_time": recorded_time,
                "icao24": flight[0],
                "callsign": flight[1].strip() if flight[1] else "UNKNOWN",
                "origin_country": flight[2],
                "longitude": flight[5],
                "latitude": flight[6],
                "altitude": flight[7],
                "velocity": flight[9]
            }

            producer.send(KAFKA_TOPIC, value=payload)

            flight_count += 1

        producer.flush()

        print(f"✓ Sent {flight_count} flights")

        time.sleep(60)

    except requests.exceptions.HTTPError as http_err:
        if http_err.response.status_code == 401:
            print("Token expired! Fetching a new access token...")
            try:
                token = get_access_token()
                print("Token refreshed successfully. Continuing stream...")
            except Exception as token_err:
                print(f"Failed to refresh token: {token_err}")
                time.sleep(60)
        else:
            print(f"HTTP Error: {http_err}")
            print("Retrying in 60 seconds...")
            time.sleep(60)

    except Exception as e:

        print(f"Error: {e}")

        print("Retrying in 60 seconds...")

        time.sleep(60)