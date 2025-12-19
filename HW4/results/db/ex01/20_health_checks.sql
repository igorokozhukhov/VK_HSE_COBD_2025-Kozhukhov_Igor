SYSTEM FLUSH LOGS;


SELECT * FROM system.clusters ORDER BY cluster, shard_num, replica_num;
SELECT * FROM system.macros ORDER BY macro;
SELECT * FROM system.zookeeper WHERE path IN ('/clickhouse', '/clickhouse/task_queue', '/clickhouse/task_queue/ddl');
SELECT * FROM system.distributed_ddl_queue ORDER BY query_create_time DESC LIMIT 50;
SELECT * FROM system.replication_queue ORDER BY create_time DESC LIMIT 50;
SELECT * FROM system.trace_log ORDER BY event_time DESC LIMIT 50;


SELECT getMacro('shard') AS shard, getMacro('replica') AS replica;
SELECT hostName() AS host, *
FROM clusterAllReplicas('cluster_1s_2r', system.macros)
ORDER BY host, macro;


