#!/bin/bash

set -o errexit
set -o nounset

if [[ $(whoami) != "root" ]]; then
	echo "This script must be run as root" >&2
	exit 1
fi

# set maximum number of open file descriptors
ulimit -n 1048576

# set up local disks used by HDFS and Yarn

# partition disks
parted /dev/xvdb mklabel gpt
parted /dev/xvdc mklabel gpt
parted /dev/xvdb mkpart P1 ext4 2048s 100%
parted /dev/xvdc mkpart P1 ext4 2048s 100%

# format partitions.  -m is for reserved block percentage (set to 0%)
mkfs.ext4 -m 0 /dev/xvdb1
mkfs.ext4 -m 0 /dev/xvdc1

# mount them
mount /dev/xvdb1 /data/data01
mount /dev/xvdc1 /data/data02

chmod -R 1777 /data/data0{1,2}
mkdir /data/data0{1,2}/tmp
chown admin.admin /data/data0{1,2}/tmp

# The master node only, needs to mount the data directory 
# if [[ "${HOSTNAME}" == "master" ]]; then ... how do we know whether we're running on the master?

printf "=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=\n" >&2
printf "If you're on the master node, mount the input data drive.\n" >&2
printf "Copy & paster this command:   mount -o ro /dev/xvdp /dataset\n" >&2
printf "=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=\n" >&2
