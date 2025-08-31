#!/usr/bin/env python3
"""
Update Kafka Connect MongoDB sink connector for analytics workloads.

This configuration extracts JSON payload strings from Kafka Connect envelopes
and stores them as structured objects in MongoDB for analytics processing.

Key features:
- Extracts 'payload' field from Kafka Connect record envelope
- Parses JSON strings into native BSON objects
- Dedicated analytics database/collection
- Uses document _id from payload for deduplication
- Error tolerance for high-throughput analytics
"""

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

    # Connector configuration optimized for analytics
    config = {
        "name": "mongo-sink",
        "config": {
            "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
            "connection.uri": mongodb_uri,
            "database": "analytics",
            "collection": "events",
            "topics": "quickstart.sampleData",
            # JSON converters to preserve object structure
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
            # Extract JSON string from Kafka Connect envelope
            "transforms": "extractPayload",
            "transforms.extractPayload.type": "org.apache.kafka.connect.transforms.ExtractField$Value",
            "transforms.extractPayload.field": "payload",
            # MongoDB JSON parsing configuration
            "json.output.decimal.format": "DECIMAL128",
            # Document ID strategy for analytics
            "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInValueStrategy",
            "document.id.strategy.overwrite.existing": "true",
            # Error handling for analytics reliability
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true",
            # Performance optimizations for analytics workload
            "max.num.retries": "3",
            "retries.defer.timeout": "5000",
            "max.batch.size": "100",
        },
    }

    try:
        # Remove existing connector
        requests.delete(f"{kafka_connect_url}/connectors/mongo-sink")
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
