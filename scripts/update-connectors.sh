# Remove all connectors from Kafka Connect using HTTP API
curl -X DELETE http://localhost:8083/connectors/mongo-source

# Register the connector
curl -X POST -H "Content-Type: application/json" \
  --data @config/source-connector-config.json \
  http://localhost:8083/connectors