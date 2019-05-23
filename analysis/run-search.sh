#!/bin/bash

module load spark
spark-shell -i Search.scala --driver-memory 120G --packages "com.github.helgeho:archivespark_2.11:2.7.6" --conf spark.executor.heartbeatInterval=10000000 --conf spark.network.timeout=10000000
