#!/usr/bin/env python3
"""
Kafka Topic Monitor for Load Testing
Monitors Kafka topics to verify event throughput from MongoDB source connector
"""

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError

    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logging.warning(
        "kafka-python not available. Install with: pip install kafka-python"
    )

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class MonitorConfig:
    """Configuration for Kafka monitoring"""

    kafka_servers: List[str] = None
    topic_pattern: str = "quickstart.sampleData"
    consumer_group: str = "load_test_monitor"
    duration_seconds: int = 60
    report_interval: int = 5

    def __post_init__(self):
        if self.kafka_servers is None:
            self.kafka_servers = ["kafka-kraft:9092"]


@dataclass
class MonitorMetrics:
    """Metrics for Kafka monitoring"""

    total_messages: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    message_timestamps: List[datetime] = None

    def __post_init__(self):
        if self.message_timestamps is None:
            self.message_timestamps = []


class KafkaMonitor:
    """Monitors Kafka topics for event throughput"""

    def __init__(self, config: MonitorConfig):
        self.config = config
        self.metrics = MonitorMetrics()
        self.consumer: Optional[KafkaConsumer] = None
        self.running = True

        if not KAFKA_AVAILABLE:
            raise ImportError(
                "kafka-python package is required. Install with: pip install kafka-python"
            )

    def setup_consumer(self) -> bool:
        """Setup Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.config.kafka_servers,
                group_id=self.config.consumer_group,
                value_deserializer=lambda x: json.loads(x.decode("utf-8"))
                if x
                else None,
                auto_offset_reset="latest",  # Start from latest messages
                enable_auto_commit=True,
                consumer_timeout_ms=1000,  # 1 second timeout for polling
            )

            # Subscribe to the topic pattern
            logger.info(f"Setting up consumer for servers: {self.config.kafka_servers}")
            logger.info(f"Looking for topics matching: {self.config.topic_pattern}")

            # Subscribe to specific topic
            self.consumer.subscribe([self.config.topic_pattern])

            logger.info("Kafka consumer setup completed")
            return True

        except KafkaError as e:
            logger.error(f"Failed to setup Kafka consumer: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to setup Kafka consumer: {e}")
            return False

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""

        def signal_handler(signum, frame):
            logger.info("Received interrupt signal, shutting down monitor...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def process_message(self, message) -> bool:
        """Process a single Kafka message"""
        try:
            # Record message receipt
            self.metrics.total_messages += 1
            self.metrics.message_timestamps.append(datetime.now(timezone.utc))

            # Log message details (optional, for debugging)
            if self.metrics.total_messages <= 5:  # Log first few messages
                logger.debug(
                    f"Message {self.metrics.total_messages}: "
                    f"Topic={message.topic}, "
                    f"Partition={message.partition}, "
                    f"Offset={message.offset}"
                )

            return True

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False

    def report_progress(self, time_elapsed: float, messages_in_period: int):
        """Report current monitoring progress"""
        current_rate = messages_in_period / time_elapsed if time_elapsed > 0 else 0

        logger.info(
            f"Messages received: {self.metrics.total_messages:,} | "
            f"Rate: {current_rate:.1f}/sec | "
            f"Total time: {(datetime.now(timezone.utc) - self.metrics.start_time).total_seconds():.1f}s"
        )

    async def monitor_topic(self):
        """Main monitoring loop"""
        if not self.setup_consumer():
            return False

        self.setup_signal_handlers()

        logger.info(
            f"Starting Kafka topic monitoring for {self.config.duration_seconds} seconds"
        )
        logger.info(f"Monitoring topic: {self.config.topic_pattern}")

        self.metrics.start_time = datetime.now(timezone.utc)
        last_report_time = time.time()
        last_report_messages = 0

        end_time = time.time() + self.config.duration_seconds

        message_batch = self.consumer.poll(timeout_ms=1000)

        try:
            while time.time() < end_time and self.running:
                if not self.consumer:
                    logger.error("Consumer is None, reconnecting...")
                    if not self.setup_consumer():
                        break
                    continue

                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=1000)

                # Process all messages in the batch
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        self.process_message(message)

                # Report progress periodically
                current_time = time.time()
                if current_time - last_report_time >= self.config.report_interval:
                    messages_in_period = (
                        self.metrics.total_messages - last_report_messages
                    )
                    self.report_progress(
                        current_time - last_report_time, messages_in_period
                    )
                    last_report_time = current_time
                    last_report_messages = self.metrics.total_messages

                # Small async sleep to allow for signal handling
                await asyncio.sleep(0.1)

        except Exception as e:
            logger.error(f"Error during monitoring: {e}")
            return False

        finally:
            self.metrics.end_time = datetime.now(timezone.utc)

        return True

    def generate_final_report(self):
        """Generate final monitoring report"""
        if not self.metrics.start_time or not self.metrics.end_time:
            logger.error("Monitor metrics incomplete")
            return

        duration = (self.metrics.end_time - self.metrics.start_time).total_seconds()
        message_rate = self.metrics.total_messages / duration if duration > 0 else 0

        print("\n" + "=" * 60)
        print("KAFKA MONITOR FINAL REPORT")
        print("=" * 60)
        print(f"Monitoring Duration: {duration:.2f} seconds")
        print(f"Total Messages Received: {self.metrics.total_messages:,}")
        print(f"Average Message Rate: {message_rate:.2f} messages/sec")
        print(f"Topic: {self.config.topic_pattern}")
        print(f"Kafka Servers: {', '.join(self.config.kafka_servers)}")

        if self.metrics.total_messages > 0:
            print(f"First Message: {self.metrics.message_timestamps[0]}")
            print(f"Last Message: {self.metrics.message_timestamps[-1]}")

        # Assessment
        if message_rate >= self.config.events_per_second * 0.9375:  # 93.75% of 800
            print(
                f"\n✅ SUCCESS: High throughput achieved ({message_rate:.1f} msg/sec)"
            )
        elif message_rate >= self.config.events_per_second * 0.8:  # 80% of 800
            print(f"\n⚠️  WARNING: Moderate throughput ({message_rate:.1f} msg/sec)")
        else:
            print(f"\n❌ LOW THROUGHPUT: Only {message_rate:.1f} msg/sec")

        print("=" * 60)

    def cleanup(self):
        """Cleanup resources"""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Kafka Topic Monitor for Load Testing")
    parser.add_argument(
        "--kafka-servers",
        nargs="+",
        default=["kafka-kraft:9092"],
        help="Kafka bootstrap servers (default: kafka-kraft:9092)",
    )
    parser.add_argument(
        "--topic",
        default="quickstart.sampleData",
        help="Topic to monitor (default: quickstart.sampleData)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Monitor duration in seconds (default: 60)",
    )
    parser.add_argument(
        "--consumer-group",
        default="load_test_monitor",
        help="Consumer group ID (default: load_test_monitor)",
    )

    args = parser.parse_args()

    if not KAFKA_AVAILABLE:
        print("Error: kafka-python package is required")
        print("Install with: pip install kafka-python")
        sys.exit(1)

    config = MonitorConfig(
        kafka_servers=args.kafka_servers,
        topic_pattern=args.topic,
        duration_seconds=args.duration,
        consumer_group=args.consumer_group,
    )

    monitor = KafkaMonitor(config)

    try:
        success = await monitor.monitor_topic()
        if success:
            monitor.generate_final_report()
        else:
            logger.error("Kafka monitoring failed to complete")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Monitor failed with exception: {e}")
        sys.exit(1)
    finally:
        monitor.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
