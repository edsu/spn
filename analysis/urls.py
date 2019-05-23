#!/usr/bin/env python

"""
Write all the URLs that were archived as text files:

    spark-submit urls.py

"""

import glob
from warc_spark import init, extractor

sc, sqlc = init()

@extractor
def url(record):
    if record.rec_type == 'response':
        yield (record.rec_headers.get_header('WARC-Target-URI'), )

warc_files = glob.glob("warcs/*/*.warc.gz")
warcs = sc.parallelize(warc_files)
output = warcs.mapPartitions(url)

output.toDF(["url"]).write.csv("out/urls")
