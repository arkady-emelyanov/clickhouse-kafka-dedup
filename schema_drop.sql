-- dictionary
DROP DICTIONARY IF EXISTS devices_dict ON CLUSTER 'my_cluster' SYNC;

-- devices
DROP TABLE IF EXISTS devices_local ON CLUSTER 'my_cluster' SYNC;
DROP TABLE IF EXISTS devices ON CLUSTER 'my_cluster' SYNC;

-- events
DROP TABLE IF EXISTS events_mv ON CLUSTER 'my_cluster' SYNC;
DROP TABLE IF EXISTS events_local ON CLUSTER 'my_cluster' SYNC;
DROP TABLE IF EXISTS events ON CLUSTER 'my_cluster' SYNC;

-- ingestion stream
DROP TABLE IF EXISTS events_stream ON CLUSTER 'my_cluster' SYNC;
