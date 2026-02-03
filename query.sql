-- total count of records in the latest sensors table
SELECT count() FROM sensors_latest;


--- count of ALERTING sensors by their latest severity
SELECT 
    Severity, 
    count() 
FROM (
    SELECT 
        SensorID, 
        argMax(Severity, UpdatedAt) as Severity,
        argMax(Status, UpdatedAt) as Status
    FROM sensors_latest
    GROUP BY SensorID
) 
WHERE Status = 'ALERTING'
GROUP BY Severity;

--- current state of all sensors (alternative method)
SELECT
    argMaxMerge(Severity) AS Severity,
    argMaxMerge(Status) AS Status
FROM sensors_state_agg
GROUP BY SensorID;
