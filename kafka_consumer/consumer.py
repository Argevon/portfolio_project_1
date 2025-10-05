import os
import time
import json
from kafka import KafkaConsumer
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "json_data")

AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER", "kafka-data")

# Initialize Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

# Initialize Azure Blob service
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(AZURE_CONTAINER)

# Create container if it doesn't exist
try:
    container_client.create_container()
except Exception:
    pass

print("Consumer started, waiting for messages...")

for message in consumer:
    try:
        data = message.value
        print("Received message:", data)

        # Use a field like 'lastUpdate' if available for blob naming, else timestamp
        last_update = data.get('lastUpdate') if isinstance(data, dict) else None
        if last_update:
            blob_name = f"ZTM_data_{last_update.replace(':','-').replace('T','_').replace('Z','')}.json"
        else:
            # fallback: use UTC timestamp
            from datetime import datetime
            blob_name = f"ZTM_data_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}.json"

        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json.dumps(data), overwrite=True)
        print(f"Blob '{blob_name}' successfully uploaded to container '{AZURE_CONTAINER}'.")

    except Exception as e:
        print(f"Error uploading blob to Azure: {e}")
        time.sleep(5)  # wait a few seconds before continuing
