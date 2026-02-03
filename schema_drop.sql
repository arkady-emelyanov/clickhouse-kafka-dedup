--
DROP TABLE IF EXISTS sensors_latest_mv ON CLUSTER 'my_cluster' SYNC;
DROP TABLE IF EXISTS sensors_latest_local ON CLUSTER 'my_cluster' SYNC;
DROP TABLE IF EXISTS sensors_latest ON CLUSTER 'my_cluster' SYNC;

---
DROP TABLE IF EXISTS sensors_latest_agg_mv ON CLUSTER 'my_cluster' SYNC;
DROP TABLE IF EXISTS sensors_latest_agg_local ON CLUSTER 'my_cluster' SYNC;
DROP TABLE IF EXISTS sensors_latest_agg ON CLUSTER 'my_cluster' SYNC;


-- Drop stream last
DROP TABLE IF EXISTS sensors_kafka_stream ON CLUSTER 'my_cluster' SYNC;
