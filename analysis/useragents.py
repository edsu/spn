#!/usr/bin/env python
# coding: utf-8

# # User Agents
# 
# This notebook examines the User Agents that were used when asking for content to be saved at Internet Archive's SavePageNow.

# In[1]:


from warc_spark import init, extractor
sc, sqlc = init()


# An extractor function to get the User-Agent from the WARC Request objects:

# In[2]:


@extractor
def ua(rec):
    if rec.rec_type == 'request':
        ua = rec.http_headers.get('user-agent')
        if ua:
            yield (ua,)


# We're going to examine them year by year.

# In[ ]:


from glob import glob

for year in range(2013, 2019):
    warc_files = glob("warcs/liveweb-{}*/*warc.gz".format(year))
    if len(warc_files) == 0:
        continue
    warcs = sc.parallelize(warc_files)

    output = warcs.mapPartitions(ua)
    df = output.toDF(["ua"])
    df.write.csv("out/ua-{}".format(year))    


# In[ ]:




