CREATE TABLE IF NOT EXISTS sensor_data (
    SensorID UUID,
    StartTime DateTime64(3),
    EndTime Nullable(DateTime64(3)),
    Status Enum8('OK' = 1, 'ALERTING' = 2),
    Severity Nullable(Enum8('LOW' = 1, 'WARNING' = 2, 'HIGH' = 3, 'CRITICAL' = 4)),
    update_ts DateTime64(3) -- Versioning column for ReplacingMergeTree
) ENGINE = ReplacingMergeTree(update_ts)
ORDER BY (SensorID, StartTime);
