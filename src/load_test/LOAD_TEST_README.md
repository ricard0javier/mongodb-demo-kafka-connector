# MongoDB Kafka Connector Load Test

This directory contains scripts to test your MongoDB Kafka connector's ability to handle 800 events per second.

## Scripts Overview

### 1. `load_test.py` - Core Load Generator

- Generates and inserts events into MongoDB at specified rate (default: 800/sec)
- Monitors insertion performance and provides detailed metrics
- Configurable batch sizes, duration, and target rates

### 2. `kafka_monitor.py` - Kafka Topic Monitor

- Monitors Kafka topics to verify events are flowing through the connector
- Measures actual message throughput from the connector
- Requires `kafka-python` package

### 3. `run_load_test.py` - Complete Test Runner

- Runs both load generation and monitoring simultaneously
- Provides comprehensive reports comparing source vs destination throughput
- Includes prerequisite checks

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Your Stack

```bash
# From project root
docker-compose up -d
```

### 3. Run the Load Test

```bash
# Basic test (800 events/sec for 60 seconds)
python src/run_load_test.py

# Custom configuration
python src/run_load_test.py --events-per-second 1000 --duration 120 --batch-size 50
```

## Usage Examples

### Load Test Only

```bash
python src/load_test.py --events-per-second 800 --duration 60
```

### Kafka Monitoring Only

```bash
python src/kafka_monitor.py --topic quickstart.sampleData --duration 60
```

### Complete Test with Custom Settings

```bash
python src/run_load_test.py \
  --events-per-second 800 \
  --duration 120 \
  --batch-size 100 \
  --mongodb-uri "mongodb://admin:admin@localhost:47027/?directConnection=true" \
  --kafka-topic "quickstart.sampleData"
```

## Configuration Options

### Load Test Parameters

- `--events-per-second`: Target event generation rate (default: 800)
- `--duration`: Test duration in seconds (default: 60)
- `--batch-size`: MongoDB insert batch size (default: 100)
- `--mongodb-uri`: MongoDB connection string
- `--database`: Target database name (default: quickstart)
- `--collection`: Target collection name (default: sampleData)

### Monitoring Parameters

- `--kafka-servers`: Kafka bootstrap servers (default: kafka-kraft:9092)
- `--kafka-topic`: Topic to monitor (default: quickstart.sampleData)

## Understanding the Results

### Load Test Metrics

- **Target Rate**: Your specified events per second
- **Actual Rate**: Measured insertion rate
- **Rate Achievement**: Percentage of target achieved
- **Success Rate**: Percentage of successful inserts
- **Batch Timing**: Average and max batch insert times

### Kafka Monitor Metrics

- **Message Rate**: Messages per second flowing through Kafka
- **Total Messages**: Total messages observed
- **Timing**: First and last message timestamps

### Success Criteria

- ✅ **SUCCESS**: ≥95% of target rate achieved
- ⚠️ **WARNING**: 85-95% of target rate
- ❌ **FAILED**: <85% of target rate

## Troubleshooting

### Common Issues

**MongoDB Connection Failed**

```bash
# Check if MongoDB is running
docker ps | grep mongodb

# Check port mapping
netstat -an | grep 47027
```

**Kafka Connection Failed**

```bash
# Check if Kafka is running
docker ps | grep kafka

# Check if connector is registered
curl http://localhost:8083/connectors
```

**Low Throughput**

- Increase batch size (`--batch-size 200`)
- Check MongoDB/Kafka resource usage
- Verify connector configuration
- Monitor network latency

### Performance Tuning

**For Higher Throughput:**

1. Increase batch size (100-500)
2. Use multiple parallel load generators
3. Optimize MongoDB write concern
4. Tune Kafka connector settings

**For Lower Latency:**

1. Decrease batch size (10-50)
2. Reduce connector polling intervals
3. Use faster storage for MongoDB

## Event Schema

Generated events follow this structure:

```json
{
  "event_id": 12345,
  "timestamp": "2024-01-01T12:00:00Z",
  "user_id": "user_123",
  "action": "page_view",
  "page": "/page/42",
  "session_id": "session_1234",
  "metadata": {
    "browser": "Chrome",
    "device": "desktop",
    "location": "US"
  },
  "value": 18517.5
}
```

## Integration with CI/CD

You can integrate these tests into your CI/CD pipeline:

```bash
#!/bin/bash
# ci-load-test.sh

# Start infrastructure
docker-compose up -d

# Wait for services
sleep 30

# Run load test
python src/run_load_test.py --duration 30 --events-per-second 800

# Check exit code
if [ $? -eq 0 ]; then
    echo "Load test PASSED"
    exit 0
else
    echo "Load test FAILED"
    exit 1
fi
```

# Results

## M20

================================================================================
COMPREHENSIVE LOAD TEST RESULTS
================================================================================

============================================================
LOAD TEST FINAL REPORT
============================================================
Duration: 61.02 seconds
Target Rate: 800 events/sec
Actual Rate: 70.47 events/sec
Rate Achievement: 8.8%
Total Events Generated: 4,300
Successful Inserts: 4,300
Failed Inserts: 0
Success Rate: 100.00%
Average Batch Time: 0.039s
Max Batch Time: 0.364s
Total Batches: 43

# ❌ FAILED: Only achieved 8.8% of target rate

============================================================
KAFKA MONITOR FINAL REPORT
============================================================
Monitoring Duration: 70.94 seconds
Total Messages Received: 0
Average Message Rate: 0.00 messages/sec
Topic: quickstart.sampleData
Kafka Servers: kafka-kraft:9092

# ❌ LOW THROUGHPUT: Only 0.0 msg/sec

## M30

================================================================================
COMPREHENSIVE LOAD TEST RESULTS
================================================================================

============================================================
LOAD TEST FINAL REPORT
============================================================
Duration: 60.12 seconds
Target Rate: 800 events/sec
Actual Rate: 765.14 events/sec
Rate Achievement: 95.6%
Total Events Generated: 46,000
Successful Inserts: 46,000
Failed Inserts: 0
Success Rate: 100.00%
Average Batch Time: 0.026s
Max Batch Time: 0.387s
Total Batches: 460

# ✅ SUCCESS: Achieved 95.6% of target rate

============================================================
KAFKA MONITOR FINAL REPORT
============================================================
Monitoring Duration: 70.06 seconds
Total Messages Received: 45,900
Average Message Rate: 655.19 messages/sec
Topic: quickstart.sampleData
Kafka Servers: kafka-kraft:9092
First Message: 2025-08-23 22:36:04.344212+00:00
Last Message: 2025-08-23 22:37:02.801550+00:00

# ⚠️ WARNING: Moderate throughput (655.2 msg/sec)

## M30 with first configuration

================================================================================
COMPREHENSIVE LOAD TEST RESULTS
================================================================================

============================================================
LOAD TEST FINAL REPORT
============================================================
Duration: 60.04 seconds
Target Rate: 800 events/sec
Actual Rate: 774.51 events/sec
Rate Achievement: 96.8%
Total Events Generated: 46,500
Successful Inserts: 46,500
Failed Inserts: 0
Success Rate: 100.00%
Average Batch Time: 0.025s
Max Batch Time: 0.038s
Total Batches: 465

# ✅ SUCCESS: Achieved 96.8% of target rate

============================================================
KAFKA MONITOR FINAL REPORT
============================================================
Monitoring Duration: 70.04 seconds
Total Messages Received: 46,400
Average Message Rate: 662.48 messages/sec
Topic: quickstart.sampleData
Kafka Servers: kafka-kraft:9092
First Message: 2025-08-23 23:02:32.654000+00:00
Last Message: 2025-08-23 23:03:31.108169+00:00

# ⚠️ WARNING: Moderate throughput (662.5 msg/sec)

## M30 with 2nd configuration without producer overwrites

================================================================================
COMPREHENSIVE LOAD TEST RESULTS
================================================================================

============================================================
LOAD TEST FINAL REPORT
============================================================
Duration: 60.13 seconds
Target Rate: 800 events/sec
Actual Rate: 768.39 events/sec
Rate Achievement: 96.0%
Total Events Generated: 46,200
Successful Inserts: 46,200
Failed Inserts: 0
Success Rate: 100.00%
Average Batch Time: 0.021s
Max Batch Time: 0.037s
Total Batches: 462

# ✅ SUCCESS: Achieved 96.0% of target rate

============================================================
KAFKA MONITOR FINAL REPORT
============================================================
Monitoring Duration: 70.12 seconds
Total Messages Received: 46,100
Average Message Rate: 657.48 messages/sec
Topic: quickstart.sampleData
Kafka Servers: kafka-kraft:9092
First Message: 2025-08-23 23:16:57.651463+00:00
Last Message: 2025-08-23 23:17:55.646320+00:00

# ⚠️ WARNING: Moderate throughput (657.5 msg/sec)

## ## M30 with 2nd configuration with producer overwrites
