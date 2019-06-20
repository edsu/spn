#!/usr/bin/env python
# coding: utf-8

# # Throughput
# 
# We can calculate the requests per second for the days of data we have. We could parse the WARC data to look at the request records. But for efficiency we can use the CDX index file and assume that every response has a corresponding request.

# In[53]:


import glob

cdx_files = glob.glob('warcs/liveweb-*/*cdx.gz')
len(cdx_files)


# We are going to need Spark to sort, since the CDX isn't ordered by time but by URL.

# In[ ]:


import sys

sys.path.append('../utils')
from warc_spark import init

sc, sqlc = init()


# Here's a somewhat convoluted function that reads a set of cdx_files, opens them and returns an iterator for all the timestamps in the CDX files. We will use this function with Spark in a second.

# In[51]:


import io
import gzip

def get_times(cdx_files):
    for cdx_file in cdx_files:
        with gzip.open(cdx_file, 'rb') as gz:
            fh = io.BufferedReader(gz)
            first = True
            for line in fh.readlines():
                # skip the first line in each file (header)
                if first:
                    first = False
                    continue
                parts = line.decode().split(" ")
                yield (parts[1],)


# Use Spark to read all the cdx files for 2018. 

# In[49]:


cdx = sc.parallelize(cdx_files)
times = cdx.mapPartitions(get_times)
times.take(5)


# In[ ]:


sorted_times = output.sortBy(lambda r: r[0])
df = sorted_times.toDF(['time'])
df.write.csv('results/times')


# In[ ]:




