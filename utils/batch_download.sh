#!/bin/bash

#sbatch -A tr5fp9p -t 48:00:00 -N 1 -C EGRESS -p RM-shared download.sh
sbatch -t 48:00:00 -N 1 -C EGRESS -p RM-shared download.sh
