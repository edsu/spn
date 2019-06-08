This directory contains some experiments of using different approaches with
Spark for analyzing WARC data. The Archives Unleashed Toolkit, ArchiveSpark, and
pyspark.

Notes:

To get these to work please create a symlink to WARC storage director, e.g.

    ln -s /pylon5/ec5fp4p/edsu/spn warcs

It can also be handy to symlink the results directory:

    ln -s ../notebooks/results results

You can run the python code on XSEDE like this:

    pyspark.sh urls.py

Many of the python scripts are actually just Jupyter notebooks that have been
converted to a script so they can run as a batch job on XSEDE.
