"""
Some functions to write pyspark jobs to extract things from WARC files. You may
want to set the SPARK_HOME envionment variable before importing this 
if things don't appear to work.
"""

import os
import sys
import glob
import warcio
import findspark

def extractor(f):
    """
    A decorator for for functions that need to extract data from WARC records.
    See Spark.ipynb also in this directory for more.
    """
    def new_f(warc_files):
        for warc_file in warc_files:
            with open(warc_file, 'rb') as stream:
                for record in warcio.ArchiveIterator(stream):
                    yield from f(record)
    return new_f

def init():
    # xsede specific configuration
    if os.path.isdir('/opt/packages/spark/latest'):
        os.environ['SPARK_HOME'] = '/opt/packages/spark/latest'
        sys.path.append("/opt/packages/spark/latest/python/lib/py4j-0.10.4-src.zip")
        sys.path.append("/opt/packages/spark/latest/python/")
        sys.path.append("/opt/packages/spark/latest/python/pyspark")

    findspark.init()
    import pyspark
    sc = pyspark.SparkContext(appName="warc-analysis")
    sqlc = pyspark.sql.SparkSession(sc)
    return sc, sqlc
