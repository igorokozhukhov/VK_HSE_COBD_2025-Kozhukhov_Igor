#!/usr/bin/env bash
set -euo pipefail


hdfs dfs -ls / >/dev/null 2>&1 || { echo "HDFS недоступен"; exit 1; }


hdfs dfs -mkdir -p /createme || true


echo "[OK] /createme создана (или уже существовала)."

hdfs dfs -test -d /delme && hdfs dfs -rm -r -f /delme || true


echo "[OK] /delme удалена (если была)."

echo "Some non-empty content $(date)" > /tmp/nonnull.txt
hdfs dfs -put -f /tmp/nonnull.txt /nonnull.txt
rm -f /tmp/nonnull.txt


echo "[OK] /nonnull.txt создан."


if ! hdfs dfs -test -f /shadow.txt; then
if [ -f "/data/shadow.txt" ]; then
echo "[INFO] Загрузка /data/shadow.txt в HDFS как /shadow.txt"
hdfs dfs -put /data/shadow.txt /shadow.txt
else
echo "[ERROR] В HDFS нет /shadow.txt и локально отсутствует /data/shadow.txt"
exit 2
fi
fi


OUT_DIR=/tmp/wordcount_shadow
hdfs dfs -test -d ${OUT_DIR} && hdfs dfs -rm -r -f ${OUT_DIR} || true


HADOOP_EXAMPLES_JAR=$(ls -1 ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar | head -n1)


YARN_OPTS="-Dmapreduce.framework.name=yarn"
hadoop jar "$HADOOP_EXAMPLES_JAR" wordcount /shadow.txt ${OUT_DIR}


echo "[OK] WordCount завершён. Результат: ${OUT_DIR}"



TMP_COUNT=/tmp/innsmouth.count
hdfs dfs -cat ${OUT_DIR}/part-* | awk -F'\t' '$1=="Innsmouth"{print $2}' > ${TMP_COUNT}


if [ ! -s ${TMP_COUNT} ]; then
echo 0 > ${TMP_COUNT}
fi


hdfs dfs -put -f ${TMP_COUNT} /whataboutinsmouth.txt
rm -f ${TMP_COUNT}
hdfs dfs -cat /whataboutinsmouth.txt || true