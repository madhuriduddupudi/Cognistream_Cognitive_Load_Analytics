import json

import psycopg2
from kafka import KafkaConsumer


# -----------------------------
# Kafka Configuration
# -----------------------------

KAFKA_TOPIC = "developer_activity"
KAFKA_SERVER = "localhost:9092"


# -----------------------------
# PostgreSQL Configuration
# -----------------------------

DB_HOST = "localhost"
DB_PORT = 5433
DB_NAME = "cognistream"
DB_USER = "cognistream"
DB_PASSWORD = "cognistream123"


# -----------------------------
# Connect to PostgreSQL
# -----------------------------

connection = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

cursor = connection.cursor()

print("Connected to PostgreSQL.")


# -----------------------------
# Connect to Kafka
# -----------------------------

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="cognistream-postgres-consumer",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Connected to Kafka.")
print("Listening for developer activity events...\n")


# -----------------------------
# Process Kafka Events
# -----------------------------

for message in consumer:

    event = message.value

    try:

        cursor.execute(
            """
            INSERT INTO developer_activity
            (
                timestamp,
                developer_id,
                activity_type,
                application,
                project,
                duration_seconds,
                interruption,
                context_switch,
                behavior_pattern,
                session_id
            )
            VALUES
            (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            """,
            (
                event["timestamp"],
                event["developer_id"],
                event["activity_type"],
                event["application"],
                event["project"],
                event["duration_seconds"],
                event["interruption"],
                event["context_switch"],
                event["behavior_pattern"],
                event["session_id"]
            )
        )

        connection.commit()

        print(
            f"Stored: {event['developer_id']} → "
            f"{event['activity_type']}"
        )

    except Exception as error:

        connection.rollback()

        print(f"Error storing event: {error}")