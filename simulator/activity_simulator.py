import random
import time
import uuid
import json
from datetime import datetime

from kafka import KafkaProducer


# -----------------------------
# Kafka Producer Configuration
# -----------------------------

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

KAFKA_TOPIC = "developer_activity"


# -----------------------------
# Raw Event Log
# -----------------------------

LOG_FILE = "data/activity_events.jsonl"


# -----------------------------
# Developers
# -----------------------------

developers = [
    "DEV_001",
    "DEV_002",
    "DEV_003",
    "DEV_004"
]


# -----------------------------
# Cognistream Projects
# -----------------------------

projects = [
    "Cognistream Platform",
    "Cognistream Analytics",
    "Cognistream API",
    "Cognistream Dashboard"
]


# -----------------------------
# Activity Definitions
# -----------------------------

activity_details = {
    "Coding": ("VS Code", False),
    "Debugging": ("VS Code", False),
    "Code Review": ("GitHub", False),
    "Git Commit": ("Git", False),
    "Testing": ("VS Code", False),
    "Meeting": ("Microsoft Teams", True),
    "Slack": ("Slack", True),
    "Email": ("Outlook", True),
    "Break": ("System", False),
    "Idle": ("System", False)
}


# -----------------------------
# Developer Behavior Patterns
# -----------------------------

behavior_patterns = {

    "Focused": {
        "activities": [
            "Coding",
            "Coding",
            "Coding",
            "Debugging",
            "Testing",
            "Git Commit"
        ]
    },

    "Normal": {
        "activities": [
            "Coding",
            "Coding",
            "Debugging",
            "Testing",
            "Git Commit",
            "Slack",
            "Email"
        ]
    },

    "Interrupted": {
        "activities": [
            "Coding",
            "Slack",
            "Email",
            "Meeting",
            "Coding",
            "Slack",
            "Email",
            "Meeting"
        ]
    }
}


# -----------------------------
# Developer Behavior Assignment
# -----------------------------

developer_behavior = {
    "DEV_001": "Focused",
    "DEV_002": "Normal",
    "DEV_003": "Interrupted",
    "DEV_004": "Normal"
}


# -----------------------------
# Event Generator
# -----------------------------

def generate_event(developer_id, behavior):

    activity = random.choice(
        behavior_patterns[behavior]["activities"]
    )

    application, interruption = activity_details[activity]

    event = {
        "timestamp": datetime.now().isoformat(),
        "developer_id": developer_id,
        "activity_type": activity,
        "application": application,
        "project": random.choice(projects),
        "duration_seconds": random.randint(30, 600),
        "interruption": interruption,
        "context_switch": interruption,
        "behavior_pattern": behavior,
        "session_id": str(uuid.uuid4())[:8]
    }

    return event


# -----------------------------
# Start Simulator
# -----------------------------

print("Cognistream Real-Time Activity Simulator Started...")
print("Kafka Topic: developer_activity")
print("Raw Log: data/activity_events.jsonl")
print("Generating Cognistream developer activity...\n")


while True:

    developer = random.choice(developers)

    behavior = developer_behavior[developer]

    event = generate_event(developer, behavior)

    # -------------------------
    # 1. Display in Terminal
    # -------------------------

    print(event)

    # -------------------------
    # 2. Write to JSONL File
    # -------------------------

    with open(LOG_FILE, "a", encoding="utf-8") as file:

        file.write(
            json.dumps(event) + "\n"
        )

    # -------------------------
    # 3. Send to Kafka
    # -------------------------

    producer.send(
        KAFKA_TOPIC,
        value=event
    )

    producer.flush()

    # -------------------------
    # Wait before next event
    # -------------------------

    time.sleep(3)