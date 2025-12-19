#!/usr/bin/env bash
set -euo pipefail

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"

service ssh start



export HDFS_NAMENODE_DIR="${HDFS_NAMENODE_DIR:-/opt/hadoop/dfs/name}"
export HDFS_DATANODE_DIR="${HDFS_DATANODE_DIR:-/opt/hadoop/dfs/data}"

mkdir -p "$HDFS_NAMENODE_DIR" "$HDFS_DATANODE_DIR"

if [ ! -d "$HDFS_NAMENODE_DIR/current" ]; then
  echo "[INIT] Formatting NameNode..."
  hdfs namenode -format -force -nonInteractive
fi

export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root

# Старт HDFS + YARN
jps -l | grep -q org.apache.hadoop.hdfs.server.namenode.NameNode || start-dfs.sh

jps -l | grep -q org.apache.hadoop.yarn.server.resourcemanager.ResourceManager || yarn --daemon start resourcemanager
jps -l | grep -q org.apache.hadoop.yarn.server.nodemanager.NodeManager       || yarn --daemon start nodemanager



# Создам домашнюю директорию на HDFS при первом запуске
hdfs dfs -mkdir -p /user/root || true

# HDFS каталоги для Hive
hdfs dfs -mkdir -p /user/hive/warehouse /tmp/hive || true
hdfs dfs -chmod -R 1777 /tmp /tmp/hive || true
hdfs dfs -chmod -R 771 /user/hive/warehouse || true

# Инициализация схемы 
schematool -dbType derby -initSchema -verbose || true

nohup hive --service metastore -p 9083 >/opt/hive/log.metastore 2>&1 &
sleep 5
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HIVE_HOME=/opt/hive
export HADOOP_HOME=/opt/hadoop
nohup hiveserver2 --hiveconf hive.metastore.uris=thrift://localhost:9083 >/opt/hive/log.hiveserver2 2>&1 &


exec bash -lc "while true; do sleep 3600; done"

