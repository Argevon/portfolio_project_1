import os
import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
from prometheus_client import start_http_server, Counter, Gauge

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

# Prometheus metrics setup
start_http_server(8000)

MESSAGES_SENT = Counter("kafka_producer_messages_total", "Number of messages send to Kafka")
ERRORS_TOTAL = Counter("kafka_producer_errors_total", "Number of producer errors")
LAST_FETCH_TIME = Gauge("kafka_producer_last_fetch_timestamp", "Last successful fetch timestamp")

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
        MESSAGES_SENT.inc()
        LAST_FETCH_TIME.set_to_current_time()
        print("[Producer] Send to Kafka:", json_data)

    except requests.exceptions.RequestException as e:
        # Handle network and response-related errors
        print(f"Error fetching data from API: {e}")
        ERRORS_TOTAL.inc()
        time.sleep(20)  # Wait before retrying
        continue

    except json.JSONDecodeError as e:
        # Handle JSON parsing issues
        print(f"Invalid JSON response: {e}")
        ERRORS_TOTAL.inc()
        time.sleep(20)  # Wait before retrying
        continue

    except Exception as e:
        # Catch-all for unexpected errors in the fetch phase
        print(f"Unexpected error during data fetching: {e}")
        ERRORS_TOTAL.inc()
        time.sleep(20)  # Wait before retrying
        continue

    except Exception as e:
        print("[Producer] Error during sendind:", e)
        ERRORS_TOTAL.inc()
    time.sleep(FETCH_INTERVAL)