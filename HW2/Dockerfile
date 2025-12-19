FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive \
    HADOOP_VERSION=3.2.1 \
    HADOOP_HOME=/opt/hadoop \
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_LOG_DIR=/opt/hadoop/logs \
    HDFS_NAMENODE_DIR=/opt/hadoop/dfs/name \
    HDFS_DATANODE_DIR=/opt/hadoop/dfs/data

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl ca-certificates openssh-server openssh-client \
    openjdk-8-jdk sudo gosu net-tools vim procps \
    && rm -rf /var/lib/apt/lists/*

# Hadoop
RUN wget -qO /tmp/hadoop.tgz https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && mkdir -p /opt \
    && tar -xzf /tmp/hadoop.tgz -C /opt \
    && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm /tmp/hadoop.tgz \
    && mkdir -p ${HADOOP_LOG_DIR} ${HDFS_NAMENODE_DIR} ${HDFS_DATANODE_DIR} /var/run/sshd /data

# --- Hive ---
ENV HIVE_VERSION=3.1.3 \
    HIVE_HOME=/opt/hive \
    HIVE_CONF_DIR=/opt/hive/conf

RUN wget -qO /tmp/hive.tgz https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
 && tar -xzf /tmp/hive.tgz -C /opt \
 && mv /opt/apache-hive-${HIVE_VERSION}-bin ${HIVE_HOME} \
 && rm /tmp/hive.tgz

# Исправление конфликта Guava
RUN rm -f ${HIVE_HOME}/lib/guava-*.jar \
 && cp ${HADOOP_HOME}/share/hadoop/common/lib/guava-*.jar ${HIVE_HOME}/lib/ \
 || cp ${HADOOP_HOME}/share/hadoop/hdfs/lib/guava-*.jar ${HIVE_HOME}/lib/

ENV PATH=${PATH}:${HIVE_HOME}/bin:${HIVE_HOME}/sbin
# --- End Hive ---

RUN ssh-keygen -A \
    && mkdir -p /root/.ssh \
    && ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa \
    && cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys \
    && chmod 600 /root/.ssh/authorized_keys

ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin

COPY conf/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY conf/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY conf/yarn-site.xml ${HADOOP_CONF_DIR}/yarn-site.xml
COPY conf/mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml
COPY conf/hadoop-env.sh ${HADOOP_CONF_DIR}/hadoop-env.sh
COPY conf/hive-site.xml ${HIVE_HOME}/conf/hive-site.xml
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh 

EXPOSE 8020 9870 9864 9866 9867 9868 8033 8088 9083 10000

ENTRYPOINT ["/entrypoint.sh"]

