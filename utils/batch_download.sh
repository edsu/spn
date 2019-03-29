#!/bin/bash

# used to download the content using XSEDE's globus setup

sbatch -t 48:00:00 -N 1 -C EGRESS -p RM-shared download.sh
