---
CREATE TABLE sensors_kafka_stream ON CLUSTER 'my_cluster' (
    SensorID UUID,
    StartTime String,
    EndTime String,
    Status String,
    Severity String,
    UpdatedAt String
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka1:29092,kafka2:29092,kafka3:29092',
    kafka_topic_list = 'sensor-alerts',
    kafka_group_name = 'clickhouse-4',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 6;

---
--- Replicated Replacing MergeTree for latest sensor records
---

CREATE TABLE sensors_latest_local ON CLUSTER 'my_cluster' (
    SensorID UUID,
    StartTime DateTime64(9),
    EndTime Nullable(DateTime64(9)),
    Status LowCardinality(String),
    Severity LowCardinality(String),
    UpdatedAt DateTime64(9)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/sensors_latest_local',
    '{replica}', 
    UpdatedAt
)
ORDER BY (SensorID);

---
CREATE TABLE sensors_latest ON CLUSTER 'my_cluster' AS sensors_latest_local
ENGINE = Distributed(
    'my_cluster', 
    currentDatabase(),
    sensors_latest_local, 
    sipHash64(SensorID)
);

---
CREATE MATERIALIZED VIEW sensors_latest_mv ON CLUSTER 'my_cluster' 
TO sensors_latest_local
AS SELECT
    SensorID,
    parseDateTime64BestEffort(StartTime, 9) AS StartTime,
    IF(
        EndTime = '',
        CAST(NULL, 'Nullable(DateTime64(9))'),
        parseDateTime64BestEffort(EndTime, 9)
    ) AS EndTime,
    Status,
    Severity,
    parseDateTime64BestEffort(UpdatedAt, 9) AS UpdatedAt
FROM sensors_kafka_stream;


