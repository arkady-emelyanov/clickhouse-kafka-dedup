## Cluster info

```
SELECT
    cluster,
    shard_num,
    replica_num,
    host_address,
    port,
    is_local
FROM system.clusters

Query id: 9ed11219-c355-4cbd-8561-d23331b3ee97

   ┌─cluster────┬─shard_num─┬─replica_num─┬─host_address─┬─port─┬─is_local─┐
1. │ default    │         1 │           1 │ 127.0.0.1    │ 9000 │        1 │
2. │ my_cluster │         1 │           1 │ 172.19.0.8   │ 9000 │        1 │
3. │ my_cluster │         1 │           2 │ 172.19.0.6   │ 9000 │        0 │
4. │ my_cluster │         1 │           3 │ 172.19.0.7   │ 9000 │        0 │
   └────────────┴───────────┴─────────────┴──────────────┴──────┴──────────┘

4 rows in set. Elapsed: 0.001 sec. 
```

## Raw data in the table

```
SELECT count() FROM sensors_latest

Query id: 7e093859-b27e-4598-b2de-249c038956d5

   ┌──count()─┐
1. │ 12885033 │ -- 12.89 million
   └──────────┘

1 row in set. Elapsed: 0.002 sec. 
```

## Distinct number of SensorIDs in the table

```
SELECT countDistinct(SensorID) FROM sensors_latest

Query id: 60201b8c-c523-47ec-a51c-6878f2a90b6f

   ┌─countDistinct(SensorID)─┐
1. │                 4171148 │ -- 4.17 million
   └─────────────────────────┘

1 row in set. Elapsed: 0.424 sec. Processed 12.89 million rows, 206.16 MB (30.42 million rows/s., 486.70 MB/s.)
Peak memory usage: 872.91 MiB.
```

## Number of active alerts per Severity

```
SELECT
    Severity,
    count() AS sensor_count
FROM sensors_latest
FINAL
WHERE Status = 'ALERTING'
GROUP BY Severity
ORDER BY sensor_count DESC

Query id: af562b69-7c94-40f5-a4d0-844f4b5a9463

   ┌─Severity─┬─sensor_count─┐
1. │ MEDIUM   │        34295 │
2. │ LOW      │        34164 │
3. │ HIGH     │        34137 │
4. │ CRITICAL │        34030 │
   └──────────┴──────────────┘

4 rows in set. Elapsed: 0.227 sec. Processed 13.61 million rows, 353.75 MB (60.03 million rows/s., 1.56 GB/s.)
Peak memory usage: 271.40 MiB.
```