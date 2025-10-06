import os
import json
import time
from kafka import KafkaConsumer
import psycopg2
from psycopg2 import OperationalError
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "json_data")

# PostgreSQL configuration
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "kafka_data")
PG_USER = os.getenv("POSTGRES_USER", "kafka_user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "super_secure_password")

# Function to connect to Postgres with retries
def connect_postgres():
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                database=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD
            )
            print("Connected to PostgreSQL")
            return conn
        except OperationalError as e:
            print(f"Postgres connection failed: {e}. Retrying in 5s...")
            time.sleep(5)

# Initialize Postgres connection
conn = connect_postgres()
cursor = conn.cursor()

# Initialize Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

print("Consumer started, writing to PostgreSQL...")

try:
    for message in consumer:
        data = message.value
        try:
            # Ensure all keys exist
            cursor.execute(
                """
                INSERT INTO vehicle_data (
                    generated, routeShortName, tripId, routeId, headsign, vehicleCode,
                    vehicleService, vehicleId, speed, direction, delay,
                    scheduledTripStartTime, lat, lon, gpsQuality
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    data.get("generated"),
                    data.get("routeShortName"),
                    data.get("tripId"),
                    data.get("routeId"),
                    data.get("headsign"),
                    data.get("vehicleCode"),
                    data.get("vehicleService"),
                    data.get("vehicleId"),
                    data.get("speed"),
                    data.get("direction"),
                    data.get("delay"),
                    data.get("scheduledTripStartTime"),
                    data.get("lat"),
                    data.get("lon"),
                    data.get("gpsQuality"),
                )
            )
            conn.commit()
            print(f"[{datetime.utcnow()}] Inserted vehicleId {data.get('vehicleId')} into PostgreSQL")
        except Exception as e:
            print(f"Error inserting data: {e}")
            conn.rollback()
            time.sleep(1)  # Avoid spamming logs

except KeyboardInterrupt:
    print("Shutting down consumer gracefully...")

finally:
    print("Closing connections...")
    cursor.close()
    conn.close()
    consumer.close()
    print("Shutdown complete.")
