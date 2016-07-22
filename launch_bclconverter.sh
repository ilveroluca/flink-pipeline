#!/bin/bash

echo "EDIT ME!" >&2
exit 1

# launch flink with 4 nodes
yarn-session.sh -n 4 -jm 10000 -tm 160000 -s 16
# from code/bclconverter: run flink program
flink run -q -c bclconverter.bclreader.test bcl-converter-assembly-0.1.jar
