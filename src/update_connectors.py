#!/usr/bin/env python3
"""Update Kafka Connect MongoDB connector using environment variables."""

import json
import os

import requests
from dotenv import load_dotenv


def main():
    load_dotenv()

    # Get MongoDB credentials from environment
    mongodb_uri = os.getenv(
        "MONGODB_URI",
        "mongodb://admin:admin@mongodb_atlas:27017/?directConnection=true",
    )
    kafka_connect_url = os.getenv("KAFKA_CONNECT_URL", "http://kafka-connect:8083")

    # MongoDB aggregation pipeline as Python objects
    pipeline = [
        {
            "$match": {
                "operationType": {"$in": ["insert", "update", "replace", "delete"]}
            }
        },
        {"$addFields": {"fullDocument.travel": "MongoDB Kafka Connector"}},
    ]

    # Connector configuration
    config = {
        "name": "mongo-source",
        "config": {
            "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
            "connection.uri": mongodb_uri,
            "database": "quickstart",
            "collection": "sampleData",
            "startup.mode": "latest",
            "publish.full.document.only": True,
            "publish.full.document.only.tombstone.on.delete": True,
            "poll.await.time.ms": 50,
            "poll.max.batch.size": 5000,
            "batch.size": 5000,
            "producer.override.linger.ms": 5,
            "producer.override.batch.size": 65536,
            "producer.override.buffer.memory": 67108864,
            "producer.override.max.block.ms": 60000,
            "producer.override.acks": "1",
            "pipeline": json.dumps(pipeline),
        },
    }

    try:
        # Remove existing connector
        requests.delete(f"{kafka_connect_url}/connectors/mongo-source")
        print("✓ Connector removed")

        # Create new connector
        response = requests.post(
            f"{kafka_connect_url}/connectors",
            headers={"Content-Type": "application/json"},
            json=config,
        )

        if response.status_code == 201:
            print("✓ Connector created successfully")
        else:
            print(f"✗ Error: {response.status_code} - {response.text}")

    except requests.exceptions.ConnectionError:
        print(f"✗ Cannot connect to Kafka Connect ({kafka_connect_url})")
    except Exception as e:
        print(f"✗ Error: {e}")


if __name__ == "__main__":
    main()
