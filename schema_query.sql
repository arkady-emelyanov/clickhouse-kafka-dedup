-- total count of records in the latest sensors table
SELECT count() FROM sensors_latest;


-- count of unique sensors in ALERTING status grouped by severity
SELECT
    Severity,
    count() AS sensor_count
FROM sensors_latest
FINAL
WHERE Status = 'ALERTING'
GROUP BY Severity
ORDER BY sensor_count DESC;
