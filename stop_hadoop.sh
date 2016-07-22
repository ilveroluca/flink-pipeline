#!/bin/bash

set -o nounset

echo "stopping Yarn..." >&2
"${HADOOP_PREFIX}/sbin/stop-yarn.sh"

echo "Stopping HDFS..." >&2
"${HADOOP_PREFIX}/sbin/stop-dfs.sh"

echo "Done.  (Not removing data from local disks.)" >&2
