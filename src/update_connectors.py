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
        {
            "$addFields": {
                "fullDocument.travel": "MongoDB Kafka Connector",
                "timeFromEventCreationToMessagePublish": {
                    "$dateDiff": {
                        "startDate": "$fullDocument.createdAt",
                        "endDate": "$wallTime",
                        "unit": "millisecond",
                    }
                },
            }
        },
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
            # Capture Before and After States
            "publish.full.document.only": False,
            "capture.mode": "change_streams",
            # Include Before Document on Updates
            "change.stream.full.document.before.change": "whenAvailable",
            "change.stream.full.document": "updateLookup",
            # Batch/poll tuning
            "batch.size": 2000,
            "poll.max.batch.size": 2000,
            "poll.await.time.ms": 200,
            "producer.override.linger.ms": 200,
            "producer.override.batch.size": 5000000,  # 2.5Kb * 2000 * 10000 / 5 = 5Mb
            "producer.override.buffer.memory": 1000000000,  # 1GB
            "producer.override.compression.type": "lz4",
            "producer.override.acks": "1",
            "producer.override.max.in.flight.requests.per.connection": 10,
            "producer.override.retries": 10,
            # Avro Key/Value Converters with Schema Registry
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "key.converter.schemas.enable": False,
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": False,
            "value.converter.decimal.format": "NUMERIC",
            # Ensure Kafka key is stored in JSON format
            "output.schema.infer.key": True,
            "output.json.formatter": "com.mongodb.kafka.connect.source.json.formatter.SimplifiedJson",
            # Ensure the Avro schema is generated with proper field types
            "output.format.value": "schema",
            "output.schema.infer.value": True,
            "output.format.key": "json",
            "change.stream.resume.strategy": "latest",
            "offset.strategy": "latest",
            "errors.tolerance": "all",
            "errors.retry.timeout": 30000,
            "errors.retry.delay.max.ms": 1000,
            "heartbeat.interval.ms": 10000,
            "heartbeat.topic.name": "sportsbook-nextgen-mongodb-connect-cluster-heartbeats",
            # Topic Configuration
            "topic.prefix": "",
            # Transformations
            # "transforms": "forwardOtelHeaders,removeUpdatedFields,dropTopicPrefix",
            # Predicate for heartbeats
            "predicates": "isHeartbeat",
            "predicates.isHeartbeat.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
            "predicates.isHeartbeat.pattern": "sportsbook-nextgen-mongodb-connect-cluster-heartbeats",
            # Apply transforms only if NOT heartbeat
            "transforms.dropTopicPrefix.predicate": "isHeartbeat",
            "transforms.dropTopicPrefix.negate": True,
            "transforms.removeUpdatedFields.predicate": "isHeartbeat",
            "transforms.removeUpdatedFields.negate": True,
            "transforms.dropTopicPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.dropTopicPrefix.regex": "sportsbook.(.*)",
            "transforms.dropTopicPrefix.replacement": "sportsbook.json.rds.$1",
            "transforms.removeUpdatedFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.removeUpdatedFields.blacklist": "updateDescription,_id",
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
