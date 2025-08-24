#!/usr/bin/env python3
"""
Complete Load Test Runner
Runs both MongoDB load test and Kafka monitoring simultaneously
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from kafka_monitor import KAFKA_AVAILABLE, KafkaMonitor, MonitorConfig

from load_test import LoadTestConfig, LoadTester

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def run_concurrent_test(
    load_config: LoadTestConfig, monitor_config: MonitorConfig
):
    """Run load test and monitoring concurrently"""

    # Create instances
    load_tester = LoadTester(load_config)
    monitor = None

    if KAFKA_AVAILABLE:
        monitor = KafkaMonitor(monitor_config)
        logger.info("Kafka monitoring enabled")
    else:
        logger.warning("Kafka monitoring disabled (kafka-python not available)")

    try:
        # Start both tasks concurrently
        tasks = []

        # Load test task
        load_task = asyncio.create_task(load_tester.run_load_test())
        tasks.append(load_task)

        # Monitoring task (if available)
        if monitor:
            monitor_task = asyncio.create_task(monitor.monitor_topic())
            tasks.append(monitor_task)

        logger.info("Starting concurrent load test and monitoring...")

        # Wait for both to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check results
        load_success = results[0] if not isinstance(results[0], Exception) else False
        monitor_success = (
            results[1]
            if len(results) > 1 and not isinstance(results[1], Exception)
            else True
        )

        # Generate reports
        print("\n" + "=" * 80)
        print("COMPREHENSIVE LOAD TEST RESULTS")
        print("=" * 80)

        if load_success:
            load_tester.generate_final_report()
        else:
            logger.error("Load test failed")
            if isinstance(results[0], Exception):
                logger.error(f"Load test exception: {results[0]}")

        if monitor and monitor_success:
            monitor.generate_final_report()
        elif monitor:
            logger.error("Kafka monitoring failed")
            if len(results) > 1 and isinstance(results[1], Exception):
                logger.error(f"Monitor exception: {results[1]}")

        return load_success and monitor_success

    finally:
        # Cleanup
        load_tester.cleanup()
        if monitor:
            monitor.cleanup()


def check_prerequisites():
    """Check if all prerequisites are met"""
    issues = []

    # Check if MongoDB is accessible
    try:
        from pymongo import MongoClient

        client = MongoClient(
            "mongodb://admin:admin@localhost:47027/?directConnection=true",
            serverSelectionTimeoutMS=3000,
        )
        client.admin.command("ping")
        client.close()
        logger.info("✅ MongoDB connection successful")
    except Exception as e:
        issues.append(f"❌ MongoDB connection failed: {e}")

    # Check if Kafka is accessible (optional)
    if KAFKA_AVAILABLE:
        try:
            from kafka import KafkaConsumer

            consumer = KafkaConsumer(bootstrap_servers=["kafka-kraft:9092"])
            consumer.close()
            logger.info("✅ Kafka connection successful")
        except Exception as e:
            issues.append(
                f"⚠️  Kafka connection failed (monitoring will be disabled): {e}"
            )
    else:
        issues.append("⚠️  kafka-python not installed (monitoring will be disabled)")

    return issues


async def main():
    """Main entry point"""
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Complete MongoDB Kafka Connector Load Test"
    )

    # Load test parameters
    parser.add_argument(
        "--events-per-second",
        type=int,
        default=800,
        help="Target events per second (default: 800)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Test duration in seconds (default: 60)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for inserts (default: 100)",
    )

    # MongoDB parameters
    parser.add_argument(
        "--mongodb-uri",
        default=os.getenv(
            "MONGODB_URI",
            "mongodb://admin:admin@localhost:47027/?directConnection=true",
        ),
        help="MongoDB connection URI",
    )
    parser.add_argument(
        "--database",
        default="quickstart",
        help="MongoDB database name (default: quickstart)",
    )
    parser.add_argument(
        "--collection",
        default="sampleData",
        help="MongoDB collection name (default: sampleData)",
    )

    # Kafka parameters
    parser.add_argument(
        "--kafka-servers",
        nargs="+",
        default=["kafka-kraft:9092"],
        help="Kafka bootstrap servers (default: kafka-kraft:9092)",
    )
    parser.add_argument(
        "--kafka-topic",
        default="quickstart.sampleData",
        help="Kafka topic to monitor (default: quickstart.sampleData)",
    )

    # Test options
    parser.add_argument(
        "--skip-prerequisites", action="store_true", help="Skip prerequisite checks"
    )
    parser.add_argument(
        "--load-only",
        action="store_true",
        help="Run only load test, skip Kafka monitoring",
    )

    args = parser.parse_args()

    # Check prerequisites
    if not args.skip_prerequisites:
        logger.info("Checking prerequisites...")
        issues = check_prerequisites()
        if issues:
            print("\nPrerequisite Check Results:")
            for issue in issues:
                print(f"  {issue}")
            print()

    # Configure load test
    load_config = LoadTestConfig(
        events_per_second=args.events_per_second,
        duration_seconds=args.duration,
        batch_size=args.batch_size,
        mongodb_uri=args.mongodb_uri,
        database=args.database,
        collection=args.collection,
    )

    # Configure monitoring
    monitor_config = MonitorConfig(
        kafka_servers=args.kafka_servers,
        topic_pattern=args.kafka_topic,
        duration_seconds=args.duration + 10,  # Monitor slightly longer
        target_throughput=args.events_per_second,
    )

    # Run the test
    try:
        if args.load_only or not KAFKA_AVAILABLE:
            # Run load test only
            logger.info("Running load test only...")
            load_tester = LoadTester(load_config)
            try:
                success = await load_tester.run_load_test()
                if success:
                    load_tester.generate_final_report()
                    return 0 if success else 1
            finally:
                load_tester.cleanup()
        else:
            # Run both load test and monitoring
            success = await run_concurrent_test(load_config, monitor_config)
            return 0 if success else 1

    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Test failed with exception: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
