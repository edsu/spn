import glob
import warcio
import pyspark
import findspark
import pyspark.sql

import extractor from warc_spark

@extractor
def url(record):
    if record.rec_type == 'response':
        yield (record.rec_headers.get_header('WARC-Target-URI'), )

findspark.init()
sc = pyspark.SparkContext(appName="warc-analysis")
sqlc = pyspark.sql.SparkSession(sc)

warc_files = glob.glob('/pylon5/ec5fp4p/edsu/spn/*/*.warc.gz')

warcs = sc.parallelize(warc_files[0:1])
output = warcs.mapPartitions(url)

output.toDF(["url"]).write.csv("/home/edsu/spn-group/edsu/notebooks/analysis/out/urls")
