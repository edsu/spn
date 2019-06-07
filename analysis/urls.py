#!/usr/bin/env python
# coding: utf-8

# # URLs in Wayback SPN Data
# 
# In addition to looking at popular host names it also could be useful to identify popular URLs that people (or bots) archived on each day. Were there attempts to archive multiple things on the same day, and what can we possibly infer about the significance of these multiple attempts?
# 
# The trouble is that when a browser interacts with SavePageNow via the [web form](https://web.archive.org) it receive the HTML for the requested webpage which has been rewritten to include some JavaScript. This JavaScript gets the browser to request any additional resources that are needed for rendering the page (JavaScript, images, CSS, etc) through SavePageNow as well. This means that a more high-fidelity recording is made, since all the resources for a web page are needed to make it human readable.
# 
# Some of these URLs may be for things like jQuery a Content Deliver Network, or a CSS file. These aren't terribly interesting in terms of this analysis which is attempting to find duplicates in the originally requested page. One thing we can do is limit our analysis to HTML pages, or requests that come back 200 OK with a `Content-Type` HTTP header containing text/html.

# In[5]:


import sys
sys.path.append('../utils')

from warc_spark import init, extractor

sc, sqlc = init()


# In order to find the URLs it's important that we also retain the User-Agent that executed the request, since this tells us something about the person or agent who initiated SavePageNow. Unfortunately the User-Agent is in the WARC Request record, and the Content-Type of the response is in the WARC Response record. To complicate matters further SavePageNow may record a response using a *revist* record if the response is identical to a previously response. This can happen when a given URL is requested multiple times in specific time window. Luckily these three record types can be merged together using the WARC-Record-ID and the WARC-Concurrent-To WARC headers.
# 
# The `get_urls` function takes a WARC Record and depending on whether it is a request, response or revisit will return a tuple containing the record id and a dictionary with either a "ua" or "url" key (depending on the type of record). These dictionaries will be merged in the next step.

# In[8]:


import re
from urllib.parse import urlparse

@extractor
def get_urls(record, warc_file):
    
    date = record.rec_headers.get_header('WARC-Date').split('T')[0]
    
    if record.rec_type == 'request':
        id = record.rec_headers.get_header('WARC-Concurrent-To')
        ua = record.http_headers.get('user-agent')
        if id and ua:
            yield (id, {"ua": ua})
            
    elif record.rec_type in ['response', 'revisit'] and 'html' in record.http_headers.get('content-type', ''):
        id = record.rec_headers.get_header('WARC-Record-ID')
        url = record.rec_headers.get_header('WARC-Target-URI')
        status_code = record.http_headers.get_statuscode()
        
        # not all 200 OK text/html responses are for requests for HTML 
        # for example some sites return 200 OK with some HTML when an image isn't found
        # this big of logic will try to identify known image, css and javascript extensions
        # to elmiminate them from consideration.
        
        uri = urlparse(url)        
        is_dependency = re.match(r'.*\.(gif|jpg|jpeg|js|png|css)$', uri.path)
        if not is_dependency and status_code == '200' and id and url:
            yield (id, {"url": url, "date": date, 'warc_file': warc_file})


# Now we can analyze our WARC data by selecting the WARC files we want to process and applying the `get_urls` function to them.

# In[9]:


from glob import glob

warc_files = glob('warcs/*/*.warc.gz')
warcs = sc.parallelize(warc_files)
results = warcs.mapPartitions(get_urls)
results.take(5)


# Now we can use [combineByKey](http://abshinn.github.io/python/apache-spark/2014/10/11/using-combinebykey-in-apache-spark/) method to merge the dictinaries using the WARC-Record-ID as a key.

# In[ ]:


def unpack(d1, d2):
    d1.update(d2)
    return d1

# merge the dataset using the record-id
dataset = results.combineByKey(
    lambda d: d,
    unpack,
    unpack
)

# filter out non-html requests (things without a url)
dataset = dataset.filter(lambda r: 'url' in r[1] and 'ua' in r[1])

dataset.take(5)


# Finally we're going to convert our dictionaries into tuples so we can easily create a DataFrame out of them for analysis. As we don this we are also going to add two new columns for the User-Agent Family and whether it is a known bot. Some JSON files that were developed as part of the UserAgents notebook can help with this.

# In[5]:


import json
ua_families = json.load(open('results/ua-families.json'))
top_uas = json.load(open('results/top-uas.json'))

def unpack(r):
    id = r[0]
    url = r[1].get("url", "")
    ua = r[1].get("ua", "")
    date = r[1].get("date", "")
    warc_file = r[1]["warc_file"]
    ua_f = ua_families.get(ua, "")
    bot = top_uas.get(ua_f, False)
    return (id, warc_file, date, url, ua, ua_f, bot)

unpacked_dataset = dataset.map(unpack)

# Convert to a Spark DataFrame
df = unpacked_dataset.toDF(["record_id", "warc_file", "date", "url", "user_agent", "user_agent_family", "bot"])


# In[6]:


df.head(5)


# This looks good, so let's save off these results before we do any more processing.
# 
# Spark writes CSVs as separate files with a `part` prefix to a given directory. We will import `move_csv_parts` which is a little function will concatenate the parts at a new location without repeating the column headers. So before we write the results let's create a little function that will consolidate these parts as a distinct csv file.

# In[4]:


from warc_spark import move_csv_parts

df.write.csv('results/urls', header=True, codec="gzip")


# Now let's count the URLs by day and see which ones have appeared more than once. First we'll save these off as CSV.

# In[9]:


from pyspark.sql.functions import countDistinct, desc

for year in range(2013, 2019):
    date = "{}-10-25".format(year)
    urls = df.filter(df.date == date)
    
    # useful in dev where note all data is being analyzed
    if urls.count() == 0:
        continue
        
    url_counts = urls.groupBy("url").count().sort(desc('count'))
    
    # remove the long tail of things that were only requested once
    url_counts = url_counts.filter(url_counts["count"] > 1)
    
    # flatten into a single csv
    url_counts = url_counts.coalesce(1)
    url_counts.write.csv('url-counts/{}'.format(date), header=True)
    
    # move 
    move_csv_parts('url-counts/{}'.format(date), 'results/{}/url-counts.csv'.format(date))


# ## URL Analysis

# In[3]:


import pandas

urls = pandas.read_csv('results/urls.csv')


# In[ ]:




