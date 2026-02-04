---
--- Devices
---
CREATE TABLE devices_local ON CLUSTER 'my_cluster' (
    DeviceID UInt32,
    Vendor String,
    FirmwareVersion LowCardinality(String),
    ManufactureDate DateTime64(9),
    UpdatedAt DateTime64(9)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/devices_local',
    '{replica}',
    UpdatedAt
)
ORDER BY (DeviceID);

---
CREATE TABLE devices ON CLUSTER 'my_cluster' AS devices_local
ENGINE = Distributed(
    'my_cluster',
    currentDatabase(),
    devices_local,
    sipHash64(DeviceID)
);

---
CREATE OR REPLACE DICTIONARY devices_dict ON CLUSTER 'my_cluster'
(
    DeviceID UInt32,
    Vendor String,
    FirmwareVersion String,
    ManufactureDate DateTime64(9),
    UpdatedAt DateTime64(9)
)
PRIMARY KEY DeviceID
SOURCE(CLICKHOUSE(
    DB 'default'
    TABLE 'devices'
    USER 'default'
    PASSWORD 'password123'
))
LIFETIME(MIN 30 MAX 60)
LAYOUT(HASHED());

---
--- Events Stream
--- 
CREATE TABLE events_stream ON CLUSTER 'my_cluster' (
    EventID UUID,
    DeviceID UInt32,
    EventType String,
    Severity String,
    Status String,
    StartTime String,
    EndTime Nullable(String),
    UpdatedAt String
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka1:29092,kafka2:29092,kafka3:29092',
    kafka_topic_list = 'events',
    kafka_group_name = 'clickhouse-4',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 3,
    kafka_max_block_size = 65536,
    kafka_poll_timeout_ms = 500;


---
--- Events ingestion
---
CREATE TABLE events_local ON CLUSTER 'my_cluster' (
    EventID UUID,
    DeviceID UInt32,
    EventType LowCardinality(String),
    Severity LowCardinality(String),
    Status LowCardinality(String),
    StartTime DateTime64(9),
    EndTime Nullable(DateTime64(9)),
    UpdatedAt DateTime64(9)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/events_local_v2', -- zookeeper key
    '{replica}', 
    UpdatedAt
)
--PARTITION BY toDate(StartTime) -- for testing purposes, in production should be toYYYYMM(StartTime)
ORDER BY (EventID);

---
CREATE TABLE events ON CLUSTER 'my_cluster' AS events_local
ENGINE = Distributed(
    'my_cluster', 
    currentDatabase(),
    events_local,
    sipHash64(DeviceID) -- sharding key for joins with devices table
);

---
CREATE MATERIALIZED VIEW events_mv ON CLUSTER 'my_cluster' 
TO events
AS SELECT
    EventID,
    DeviceID,
    EventType,
    Severity,
    Status,
    parseDateTime64BestEffort(StartTime, 9) AS StartTime,
    parseDateTime64BestEffortOrNull(EndTime, 9) AS EndTime,
    parseDateTime64BestEffort(UpdatedAt, 9) AS UpdatedAt
FROM events_stream;
