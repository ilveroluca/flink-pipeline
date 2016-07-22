#!/bin/bash

set -o errexit
set -o nounset

n_nodes=$(grep -c -v '^#' "${HADOOP_CONF_DIR}/slaves")
echo "=================================================" >&2
echo "Starting a Hadoop cluster on the following ${n_nodes} nodes:" >&2
grep -v '^#' "${HADOOP_CONF_DIR}/slaves"
echo "=================================================" >&2

# Give the user a chance to kill the script
echo "Hit ^C now if you changed your mind" >&2
sleep 5


#mkdir /data/data0{1,2}/tmp

# clean logs
rm -rf "${HADOOP_PREFIX}/logs/hadoop*" "${HADOOP_PREFIX}/logs/yarn*" "${HADOOP_PREFIX}/logs/userlogs"

hdfs namenode -format
"${HADOOP_PREFIX}/sbin/start-dfs.sh"
hdfs fs -mkdir /user/ /user/$(whoami) /data

"${HADOOP_PREFIX}/sbin/start-yarn.sh"
