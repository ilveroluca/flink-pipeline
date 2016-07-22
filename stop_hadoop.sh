#!/bin/bash

set -o nounset

echo "Stopping history server" >&2
"${HADOOP_PREFIX}/sbin/mr-jobhistory-daemon.sh" stop historyserver

echo "stopping Yarn..." >&2
"${HADOOP_PREFIX}/sbin/stop-yarn.sh"

echo "Stopping HDFS..." >&2
"${HADOOP_PREFIX}/sbin/stop-dfs.sh"

echo "Done.  (Not removing data from local disks.)" >&2
