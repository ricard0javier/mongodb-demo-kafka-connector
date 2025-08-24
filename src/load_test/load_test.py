#!/usr/bin/env python3
"""
MongoDB Kafka Connector Load Test
Generates 800 events per second to test connector throughput
"""

import argparse
import asyncio
import logging
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean
from typing import Dict, List, Optional

from pymongo import MongoClient
from pymongo.errors import PyMongoError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class LoadTestConfig:
    """Configuration for load testing"""

    events_per_second: int = 800
    duration_seconds: int = 60
    mongodb_uri: str = "mongodb://admin:admin@localhost:47027/?directConnection=true"
    database: str = "quickstart"
    collection: str = "sampleData"
    batch_size: int = 100
    report_interval: int = 5


@dataclass
class Metrics:
    """Metrics tracking for load test"""

    total_events: int = 0
    successful_inserts: int = 0
    failed_inserts: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    batch_times: List[float] = None

    def __post_init__(self):
        if self.batch_times is None:
            self.batch_times = []


class EventGenerator:
    """Generates realistic event data for testing"""

    @staticmethod
    def generate_event(event_id: int) -> Dict:
        """Generate a single event document"""
        return {
            "event_id": event_id,
            "timestamp": datetime.now(timezone.utc),
            "user_id": f"user_{event_id % 1000}",
            "action": "page_view" if event_id % 3 == 0 else "click",
            "page": f"/page/{event_id % 50}",
            "session_id": f"session_{event_id // 10}",
            "metadata": {
                "browser": "Chrome" if event_id % 2 == 0 else "Firefox",
                "device": "mobile" if event_id % 4 == 0 else "desktop",
                "location": "US" if event_id % 3 == 0 else "EU",
            },
            "value": event_id * 1.5,
        }

    @classmethod
    def generate_batch(cls, start_id: int, batch_size: int) -> List[Dict]:
        """Generate a batch of events"""
        return [cls.generate_event(start_id + i) for i in range(batch_size)]


class LoadTester:
    """Main load testing class"""

    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.metrics = Metrics()
        self.client: Optional[MongoClient] = None
        self.collection = None
        self.running = True

        # Calculate timing parameters
        self.batches_per_second = self.config.events_per_second / self.config.batch_size
        self.seconds_per_batch = 1.0 / self.batches_per_second

        logger.info(f"Configured for {self.config.events_per_second} events/sec")
        logger.info(f"Batch size: {self.config.batch_size}")
        logger.info(f"Batches per second: {self.batches_per_second:.2f}")
        logger.info(f"Seconds per batch: {self.seconds_per_batch:.3f}")

    def connect_mongodb(self) -> bool:
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(
                self.config.mongodb_uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
            )
            # Test connection
            self.client.admin.command("ping")

            db = self.client[self.config.database]
            self.collection = db[self.config.collection]

            logger.info(
                f"Connected to MongoDB: {self.config.database}.{self.config.collection}"
            )
            return True
        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False

    def insert_batch(self, events: List[Dict]) -> bool:
        """Insert a batch of events into MongoDB"""
        try:
            start_time = time.time()
            result = self.collection.insert_many(events)
            batch_time = time.time() - start_time

            self.metrics.batch_times.append(batch_time)
            self.metrics.successful_inserts += len(result.inserted_ids)

            return True
        except PyMongoError as e:
            logger.error(f"Batch insert failed: {e}")
            self.metrics.failed_inserts += len(events)
            return False

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""

        def signal_handler(signum, frame):
            logger.info("Received interrupt signal, shutting down gracefully...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def run_load_test(self):
        """Run the main load test"""
        if not self.connect_mongodb():
            return False

        self.setup_signal_handlers()

        logger.info(f"Starting load test for {self.config.duration_seconds} seconds")

        self.metrics.start_time = datetime.now(timezone.utc)
        event_id = 0
        last_report_time = time.time()
        last_report_events = 0

        end_time = time.time() + self.config.duration_seconds

        while time.time() < end_time and self.running:
            batch_start_time = time.time()

            # Generate and insert batch
            events = EventGenerator.generate_batch(event_id, self.config.batch_size)
            self.insert_batch(events)

            event_id += self.config.batch_size
            self.metrics.total_events += self.config.batch_size

            # Report progress periodically
            current_time = time.time()
            if current_time - last_report_time >= self.config.report_interval:
                self.report_progress(
                    current_time - last_report_time,
                    self.metrics.total_events - last_report_events,
                )
                last_report_time = current_time
                last_report_events = self.metrics.total_events

            # Maintain target rate
            batch_duration = time.time() - batch_start_time
            sleep_time = max(0, self.seconds_per_batch - batch_duration)

            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            elif batch_duration > self.seconds_per_batch * 1.1:  # 10% tolerance
                logger.warning(
                    f"Batch took {batch_duration:.3f}s, target: {self.seconds_per_batch:.3f}s"
                )

        self.metrics.end_time = datetime.now(timezone.utc)
        return True

    def report_progress(self, time_elapsed: float, events_in_period: int):
        """Report current progress"""
        current_rate = events_in_period / time_elapsed
        avg_batch_time = (
            mean(self.metrics.batch_times[-20:]) if self.metrics.batch_times else 0
        )

        logger.info(
            f"Events: {self.metrics.total_events:,} | "
            f"Rate: {current_rate:.1f}/sec | "
            f"Target: {self.config.events_per_second}/sec | "
            f"Avg batch time: {avg_batch_time:.3f}s | "
            f"Success: {self.metrics.successful_inserts:,} | "
            f"Failed: {self.metrics.failed_inserts:,}"
        )

    def generate_final_report(self):
        """Generate final test report"""
        if not self.metrics.start_time or not self.metrics.end_time:
            logger.error("Test metrics incomplete")
            return

        duration = (self.metrics.end_time - self.metrics.start_time).total_seconds()
        actual_rate = self.metrics.successful_inserts / duration
        target_rate = self.config.events_per_second

        success_rate = (
            self.metrics.successful_inserts / self.metrics.total_events
        ) * 100
        avg_batch_time = (
            mean(self.metrics.batch_times) if self.metrics.batch_times else 0
        )
        max_batch_time = (
            max(self.metrics.batch_times) if self.metrics.batch_times else 0
        )

        print("\n" + "=" * 60)
        print("LOAD TEST FINAL REPORT")
        print("=" * 60)
        print(f"Duration: {duration:.2f} seconds")
        print(f"Target Rate: {target_rate:,} events/sec")
        print(f"Actual Rate: {actual_rate:.2f} events/sec")
        print(f"Rate Achievement: {(actual_rate / target_rate) * 100:.1f}%")
        print(f"Total Events Generated: {self.metrics.total_events:,}")
        print(f"Successful Inserts: {self.metrics.successful_inserts:,}")
        print(f"Failed Inserts: {self.metrics.failed_inserts:,}")
        print(f"Success Rate: {success_rate:.2f}%")
        print(f"Average Batch Time: {avg_batch_time:.3f}s")
        print(f"Max Batch Time: {max_batch_time:.3f}s")
        print(f"Total Batches: {len(self.metrics.batch_times):,}")

        # Performance assessment
        if actual_rate >= target_rate * 0.95:  # 95% of target
            print(
                f"\n✅ SUCCESS: Achieved {(actual_rate / target_rate) * 100:.1f}% of target rate"
            )
        elif actual_rate >= target_rate * 0.85:  # 85% of target
            print(
                f"\n⚠️  WARNING: Achieved {(actual_rate / target_rate) * 100:.1f}% of target rate"
            )
        else:
            print(
                f"\n❌ FAILED: Only achieved {(actual_rate / target_rate) * 100:.1f}% of target rate"
            )

        print("=" * 60)

    def cleanup(self):
        """Cleanup resources"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="MongoDB Kafka Connector Load Test")
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
    parser.add_argument(
        "--mongodb-uri",
        default="mongodb://admin:admin@localhost:47027/?directConnection=true",
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

    args = parser.parse_args()

    config = LoadTestConfig(
        events_per_second=args.events_per_second,
        duration_seconds=args.duration,
        batch_size=args.batch_size,
        mongodb_uri=args.mongodb_uri,
        database=args.database,
        collection=args.collection,
    )

    load_tester = LoadTester(config)

    try:
        success = await load_tester.run_load_test()
        if success:
            load_tester.generate_final_report()
        else:
            logger.error("Load test failed to complete")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Load test failed with exception: {e}")
        sys.exit(1)
    finally:
        load_tester.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
