#!/usr/bin/env python

"""
Write all the links that were archived as text files:

    spark-submit urls.py

"""

import glob

from bs4 import BeautifulSoup
from warc_spark import init, extractor
from urllib.parse import urljoin

sc, sqlc = init()

@extractor
def url(rec):
    if rec.rec_type == 'response':
        if 'html' in rec.http_headers.get('content-type', '') and \
                rec.http_headers.get_statuscode() == '200':
            doc = BeautifulSoup(rec.raw_stream.read())
            source = rec.rec_headers.get_header('WARC-Target-URI')
            for link in doc.find_all('a'):
                target = urljoin(source, link.get('href'))
                if source and target:
                    yield (source, target)

warc_files = glob.glob("warcs/*/*.warc.gz")
warcs = sc.parallelize(warc_files)
output = warcs.mapPartitions(url)

output.toDF(["sourcd","target"]).write.csv("out/links")
