# MongoDB Atlas Demo for Kafka Connectors

A MongoDB service and a Kafka Connector pulling updates from MongoDB into a kafka topic

Run `make clean install dev` and open [playground notebook](playground.ipynb), select the python kernel
with the environment `mongodb-demo-kafka-connector` and open the [dashboard](http://localhost:9080/ui/demo-kafka/tail?topicId=quickstart.sampleData) to monitor your changes.

When documents are created, modified or deleted from the database `quickstart` and collection `sampleData`, you should observe messages in the kafka topic `quickstart.sampleData`. Such messages are exported from
MongoDB using the Kakfa Connector.

You can add or modify the connectors using with scripts like [update.conectors.js](scripts/update-connectors.sh), and you can also explore the configuration of the demo environment looking at the docker compose file [docker-compose.yml](docker-compose.yml)

The connection string for MongoDB is:
`mongodb://admin:admin@localhost:47027/?directConnection=true`

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
