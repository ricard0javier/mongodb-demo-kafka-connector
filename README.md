# MongoDB Atlas Demo for Kafka Connectors

A MongoDB service and a Kafka Connector pulling updates from MongoDB into a kafka topic

Run `make clean install dev` and open [playground notebook](playground.ipynb), select the python kernel
with the environment `mongodb-demo-kafka-connector` and open the [dashboard](http://localhost:9080/ui/demo-kafka/tail?topicId=quickstart.sampleData) to monitor your changes.

When documents are created, modified or deleted from the database `quickstart` and collection `sampleData`, you should observe messages in the kafka topic `quickstart.sampleData`. Such messages are exported from
MongoDB using the Kakfa Connector.

You can add or modify the connectors using with scripts like [update.conectors.js](scripts/update-connectors.sh), and you can also explore the configuration of the demo environment looking at the docker compose file [docker-compose.yml](docker-compose.yml)

The connection string for MongoDB is:
`mongodb://admin:admin@localhost:47027/?directConnection=true`

The grafana dashboard is available [here](http://localhost:3000/explore?schemaVersion=1&panes=%7B%22tt3%22%3A%7B%22datasource%22%3A%22PBFA97CFB590B2093%22%2C%22queries%22%3A%5B%7B%22refId%22%3A%22A%22%2C%22expr%22%3A%22kafka_connect_source_task_metrics_source_record_write_rate%22%2C%22range%22%3Atrue%2C%22instant%22%3Atrue%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22PBFA97CFB590B2093%22%7D%2C%22editorMode%22%3A%22builder%22%2C%22legendFormat%22%3A%22__auto%22%2C%22useBackend%22%3Afalse%2C%22disableTextWrap%22%3Afalse%2C%22fullMetaSearch%22%3Afalse%2C%22includeNullMetadata%22%3Atrue%7D%2C%7B%22refId%22%3A%22B%22%2C%22expr%22%3A%22kafka_connect_source_task_metrics_source_record_poll_rate%22%2C%22range%22%3Atrue%2C%22instant%22%3Atrue%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22PBFA97CFB590B2093%22%7D%2C%22editorMode%22%3A%22builder%22%2C%22legendFormat%22%3A%22__auto%22%2C%22useBackend%22%3Afalse%2C%22disableTextWrap%22%3Afalse%2C%22fullMetaSearch%22%3Afalse%2C%22includeNullMetadata%22%3Atrue%7D%2C%7B%22refId%22%3A%22C%22%2C%22expr%22%3A%22kafka_connect_source_task_metrics_source_record_active_count_avg%22%2C%22range%22%3Atrue%2C%22instant%22%3Atrue%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22PBFA97CFB590B2093%22%7D%2C%22editorMode%22%3A%22builder%22%2C%22legendFormat%22%3A%22__auto%22%2C%22useBackend%22%3Afalse%2C%22disableTextWrap%22%3Afalse%2C%22fullMetaSearch%22%3Afalse%2C%22includeNullMetadata%22%3Atrue%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-3h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1)

## Features

- Integration with MongoDB
- Docker Compose setup for local development

## Prerequisites

- Python
- Docker and Docker Compose
- Make (optional, for using Makefile commands)

## Getting Started

Start by adding code in the [index.ts](src/main.py) or using the [playground](playground.ipynb)

### Environment Variables

#### MongoDB Configuration

- `MONGODB_URI` - MongoDB connection string (default: `mongodb://admin:admin@localhost:47027/?directConnection=true`)
- `MONGODB_DATABASE` - Database name (default: `content-discovery`)
- `MONGODB_MAX_POOL_SIZE` - Maximum connection pool size (default: `100`)
- `MONGODB_MIN_POOL_SIZE` - Minimum connection pool size (default: `10`)
- `MONGODB_MAX_IDLE_TIME_MS` - Maximum idle time for connections in ms (default: `30000`)
- `MONGODB_CONNECT_TIMEOUT_MS` - Connection timeout in ms (default: `10000`)
- `MONGODB_SERVER_SELECTION_TIMEOUT_MS` - Server selection timeout in ms (default: `5000`)
- `MONGODB_RETRY_WRITES` - Enable retry writes (default: `true`)
- `MONGODB_RETRY_READS` - Enable retry reads (default: `true`)
- `KAFKA_CONNECT_URL` - Url to connect to kafka connect anc configure connectors (default: `http://localhost:8083`)

#### Logging

- `LOG_LEVEL` - Logging level (default: `INFO`)

### Local Development

1. Clean and Install dependencies:

   ```bash
   make clean install
   ```

2. Start the development server:
   ```bash
   make dev
   ```

## Development Commands

- `make clean`: Clean build artifacts
- `make install`: Install dependencies
- `make dev`: Start development server

## Load Testing

Test your MongoDB Kafka connector's performance with built-in load testing tools.

### Quick Test

```bash
# Run complete load test (800 events/sec for 60 seconds)
python src/load_test/run_load_test.py

# Custom configuration
python src/load_test/run_load_test.py --events-per-second 4000 --duration 120
```

### Key Parameters

- `--events-per-second`: Target rate (default: 800)
- `--duration`: Test duration in seconds (default: 60)
- `--batch-size`: MongoDB insert batch size (default: 100)
- `--mongodb-uri`: Connection string
- `--kafka-topic`: Topic to monitor (default: quickstart.sampleData)

### Success Criteria

- ✅ **SUCCESS**: ≥95% target rate achieved
- ⚠️ **WARNING**: 85-95% target rate
- ❌ **FAILED**: <85% target rate

See [LOAD_TEST_README.md](src/load_test/LOAD_TEST_README.md) for detailed usage and troubleshooting.
