#!/bin/bash

module load spark
spark-shell -i $i --driver-memory 120G --packages "io.archivesunleashed:aut:0.17.0" --conf spark.executor.heartbeatInterval=10000000 --conf spark.network.timeout=10000000
