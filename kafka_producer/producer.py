import os
import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Loading variables from .env
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "json_data")
SOURCE_URL = os.getenv("SOURCE_URL", "https://ckan2.multimediagdansk.pl/gpsPositions?v=2")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "20"))

# Kafka producer configuration
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        break
    except Exception as e:
        print("Kafka not ready, retrying in 5s...", e)
        time.sleep(5)

while True:
    try:
        # Fetch JSON data from the API
        response = requests.get("https://ckan2.multimediagdansk.pl/gpsPositions?v=2")
        response.raise_for_status()  # Raise exception for HTTP status codes >= 400

        # Parse JSON data
        json_data = response.json()

        # Sending the data to Kafka producer
        producer.send(KAFKA_TOPIC, value=json_data)
        producer.flush()
        print("[Producer] Wysłano do Kafka:", json_data)

    except requests.exceptions.RequestException as e:
        # Handle network and response-related errors
        print(f"Error fetching data from API: {e}")
        time.sleep(20)  # Wait before retrying
        continue

    except json.JSONDecodeError as e:
        # Handle JSON parsing issues
        print(f"Invalid JSON response: {e}")
        time.sleep(20)  # Wait before retrying
        continue

    except Exception as e:
        # Catch-all for unexpected errors in the fetch phase
        print(f"Unexpected error during data fetching: {e}")
        time.sleep(20)  # Wait before retrying
        continue

    except Exception as e:
        print("[Producer] Błąd podczas wysyłania:", e)
    time.sleep(FETCH_INTERVAL)