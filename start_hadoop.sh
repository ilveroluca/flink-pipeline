#!/bin/bash

set -o errexit
set -o nounset

#mkdir /data/data0{1,2}/tmp

# clean logs
rm -rf "${HADOOP_PREFIX}/logs/hadoop*" "${HADOOP_PREFIX}/logs/yarn*" "${HADOOP_PREFIX}/logs/userlogs"

hdfs namenode -format
"${HADOOP_PREFIX}/sbin/start-dfs.sh"
hdfs fs -mkdir /user/ /user/$(whoami) /data

"${HADOOP_PREFIX}/sbin/start-yarn.sh"
