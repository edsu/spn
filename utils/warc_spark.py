"""
Some functions to write pyspark jobs to extract things from WARC files. You may
want to set the SPARK_HOME envionment variable before importing this 
if things don't appear to work.
"""

import os
import sys
import glob
import shutil
import warcio
import inspect
import findspark

def extractor(f):
    """
    A decorator for for functions that need to extract data from WARC records.
    See Spark.ipynb also in this directory for more.
    """

    # if the extractor function takes two arguments pass the warc file too
    spec = inspect.getfullargspec(f)
    if len(spec.args) == 2:
        include_warc_file = True
    else:
        include_warc_file = False

    def new_f(warc_files):
        for warc_file in warc_files:
            with open(warc_file, 'rb') as stream:
                for record in warcio.ArchiveIterator(stream):
                    if include_warc_file:
                        yield from f(record, warc_file)
                    else:
                        yield from f(record)
    return new_f

def init():
    # xsede specific configuration
    if os.path.isdir('/opt/packages/spark/latest'):
        os.environ['SPARK_HOME'] = '/opt/packages/spark/latest'
        sys.path.append("/opt/packages/spark/latest/python/lib/py4j-0.10.7-src.zip")
        sys.path.append("/opt/packages/spark/latest/python/")
        sys.path.append("/opt/packages/spark/latest/python/pyspark")

    findspark.init()
    import pyspark
    sc = pyspark.SparkContext(appName="warc-analysis")
    sqlc = pyspark.sql.SparkSession(sc)
    return sc, sqlc

def move_csv_parts(parts_path, csv_path):
    output = open(csv_path, 'w')
    wrote_header = False
    for filename in glob.glob(os.path.join(parts_path, 'part*.csv')):
        with open(filename) as fh:
            first = True
            for line in fh:
                if first and wrote_header:
                    first = False
                    continue
                wrote_header = True
                output.write(line)
    shutil.rmtree(parts_path)
