solar_data = {
  connectionName: "sample_stream_solar",
};

kafka_topic = {
  connectionName: "kafka_ssl",
  topic: "demo_topic",
};

// Relevant Resources:
// https://www.youtube.com/watch?v=09lBhM8XGUQ
// https://www.mongodb.com/docs/manual/reference/operator/aggregation/addFields/
// https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-meta/
// https://www.mongodb.com/docs/atlas/atlas-stream-processing/sp-agg-emit/
// https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateDiff/
extra_fields = {
  time_to_ingest_ms: {
    $dateDiff: {
      startDate: { $dateFromString: { dateString: "$timestamp" } },
      endDate: { $meta: "stream.source.ts" },
      unit: "millisecond",
    },
  },
  headers: {
    origin: "MongoDB ASP",
    UUID: { $createUUID: {} },
  },
};

sp.process([
  {
    $source: solar_data,
  },
  {
    $addFields: extra_fields,
  },
  {
    $emit: {
      ...kafka_topic,
      config: {
        headers: "$headers",
      },
    },
  },
]);
