-- count of all events
SELECT count() FROM events;

-- check device dict
SELECT name, status, element_count, last_exception 
FROM system.dictionaries 
WHERE name = 'devices_dict';

-- reload if needed
SYSTEM RELOAD DICTIONARY devices_dict;

-- check partitioning
SELECT name, partition FROM system.parts WHERE table = 'events_local' AND active;


-- count of ALERTING events grouped by Severity
SELECT Severity, count() as events_count
FROM events
FINAL
WHERE Status = 'ALERTING'
GROUP BY Severity
ORDER BY events_count DESC;

--- 
SELECT
    DeviceID,
    dictGet('devices_dict', 'Vendor', DeviceID) AS Vendor,
    dictGet('devices_dict', 'FirmwareVersion', DeviceID) AS FirmwareVersion,
    count() AS event_count
FROM events
FINAL
GROUP BY DeviceID
ORDER BY event_count DESC
LIMIT 10;


---
SELECT count(DISTINCT EventID)
FROM events
FINAL
WHERE 
    (dictGet('devices_dict', 'Vendor', DeviceID) = 'VendorD') AND 
    (dictGet('devices_dict', 'FirmwareVersion', DeviceID) = 'v1.2.0') AND
    Status = 'ALERTING' AND
    Severity = 'HIGH';

---
SELECT count(DISTINCT e.EventID)
FROM events AS e
FINAL
GLOBAL INNER JOIN devices AS d ON e.DeviceID = d.DeviceID
WHERE
    d.Vendor = 'VendorD' AND 
    d.FirmwareVersion = 'v1.2.0' AND
    e.Status = 'ALERTING' AND 
    e.Severity = 'HIGH';