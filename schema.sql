-- 1. Drop the Materialized View first (stops the ingestion flow)
DROP TABLE IF EXISTS sensors_mv ON CLUSTER 'my_cluster' SYNC;
DROP TABLE IF EXISTS sensors_kafka_stream ON CLUSTER 'my_cluster' SYNC;
DROP TABLE IF EXISTS sensors_latest ON CLUSTER 'my_cluster' SYNC;
DROP TABLE IF EXISTS sensors_latest_local ON CLUSTER 'my_cluster' SYNC;

---
CREATE TABLE sensors_latest_local ON CLUSTER 'my_cluster' (
    SensorID UUID,
    StartTime DateTime64(9),
    EndTime DateTime64(9),
    Status LowCardinality(String),
    Severity LowCardinality(String),
    UpdatedAt DateTime64(9)
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/sensors_latest_local', '{replica}', UpdatedAt)
ORDER BY SensorID;

---
CREATE TABLE sensors_latest ON CLUSTER 'my_cluster' AS sensors_latest_local
ENGINE = Distributed('my_cluster', currentDatabase(), sensors_latest_local, sipHash64(SensorID));


---
CREATE TABLE sensors_kafka_stream ON CLUSTER 'my_cluster' (
    SensorID UUID,
    StartTime Int64,
    EndTime Int64,
    Status String,
    Severity String,
    UpdatedAt Int64
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka1:29092,kafka2:29092,kafka3:29092',
    kafka_topic_list = 'sensor-alerts',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 5;


---
CREATE MATERIALIZED VIEW sensors_mv ON CLUSTER 'my_cluster' TO sensors_latest_local AS
SELECT
    SensorID,
    fromUnixTimestamp64Nano(StartTime) AS StartTime,
    fromUnixTimestamp64Nano(EndTime) AS EndTime,
    Status,
    Severity,
    fromUnixTimestamp64Nano(UpdatedAt) AS UpdatedAt
FROM sensors_kafka_stream;

--- sample query
-- SELECT 
--     Severity, 
--     count() 
-- FROM (
--     SELECT 
--         SensorID, 
--         argMax(Severity, UpdatedAt) as Severity,
--         argMax(Status, UpdatedAt) as Status
--     FROM sensors_latest
--     GROUP BY SensorID
-- ) 
-- WHERE Status = 'ALERTING'
-- GROUP BY Severity;