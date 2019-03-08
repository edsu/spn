#!/usr/bin/env python

import glob

from warcio.archiveiterator import ArchiveIterator

def process(warc_file):
    for record in ArchiveIterator(open(warc_file, 'rb')):
        if record.rec_type == 'request':
            if record.http_headers['host'] == 'www.facebook.com':
                referer = record.http_headers['referer']
                url = record.rec_headers['WARC-Target-URI']
                if not referer:
                    print(referer, url)




for filename in glob.glob('data/*/*.warc.gz'):
    process(filename)

