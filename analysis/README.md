This directory contains some experiments of using different approaches with
Spark for analyzing WARC data. The Archives Unleashed Toolkit, ArchiveSpark, and
pyspark.

Notes:

To get these to work please create a symlink to WARC storage director, e.g.

    ln -s /pylon5/ec5fp4p/edsu/spn warcs

You can the python code on XSEDE like this:

    interact
    module load spark
    spark-submit urls.py

Analyze 
