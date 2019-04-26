#!/bin/bash

spark-shell -i Search.scala --driver-memory 20G --packages "com.github.helgeho:archivespark_2.11:2.7.6"
