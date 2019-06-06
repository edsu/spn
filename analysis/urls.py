#!/usr/bin/env python
# coding: utf-8

# # URLs in Wayback SPN Data
# 
# In addition to looking at popular host names it also could be useful to identify popular URLs that people (or bots) archived on each day. Were there attempts to archive multiple things on the same day, and what can we possibly infer about the significance of these multiple attempts?
# 
# The trouble is that when a browser interacts with SavePageNow via the [web form](https://web.archive.org) it receive the HTML for the requested webpage which has been rewritten to include some JavaScript. This JavaScript gets the browser to request any additional resources that are needed for rendering the page (JavaScript, images, CSS, etc) through SavePageNow as well. This means that a more high-fidelity recording is made, since all the resources for a web page are needed to make it human readable.
# 
# Some of these URLs may be for things like jQuery a Content Deliver Network, or a CSS file. These aren't terribly interesting in terms of this analysis which is attempting to find duplicates in the originally requested page. One thing we can do is limit our analysis to HTML pages, or requests that come back 200 OK with a `Content-Type` HTTP header containing text/html.

# In[1]:


from warc_spark import init, extractor

sc, sqlc = init()


# In order to find the URLs it's important that we also retain the User-Agent that executed the request, since this tells us something about the person who initiated SavePageNow. Unfortunately the User-Agent is in the WARC Resquest record, and the Content-Type of the response is in the WARC Response record. Luckily these can be connected together using the WARC-Record-ID and the WARC-Concurrent-To WARC headers.
# 
# The `get_urls` function takes a WARC Record and depending on whether it is a request or a response will return a tuple containing the record id and either a User-Agent or a URL for a text/html response. For example:
# 
# ```
# (urn:uuid:551471a6-631b-4ef7-99a5-f1344348ab64>', 'Mozilla/5.0 (compatible; archive.org_bot; Wayback Machine Live Record; +http://archive.org/details/archive.org_bot)')
# (urn:uuid:551471a6-631b-4ef7-99a5-f1344348ab64>', 'https://yahoo.com')
#  ```
# 

# In[7]:


import re
from urllib.parse import urlparse

@extractor
def get_urls(record):
    
    if record.rec_type == 'request':
        id = record.rec_headers.get_header('WARC-Concurrent-To')
        ua = record.http_headers.get('user-agent')
        if id and ua:
            yield (id, ua)
            
    elif record.rec_type == 'response' and 'html' in record.http_headers.get('content-type', ''):
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
            yield (id, url)


# Now we can process our data by selecting the WARC files we want to process and applying the `get_urls` function to them. We then group the results by the WARC-Record-ID to yield something like:
# 
#     ('<urn:uuid:551471a6-631b-4ef7-99a5-f1344348ab64>', 'Mozilla/5.0 (compatible; archive.org_bot; Wayback Machine Live Record; +http://archive.org/details/archive.org_bot)')
#     
#     ('<urn:uuid:551471a6-631b-4ef7-99a5-f1344348ab64>', 'https://yahoo.com')

# In[8]:


from glob import glob

warc_files = glob('warcs/*/*.warc.gz')
warcs = sc.parallelize(warc_files)
results = warcs.mapPartitions(get_urls)
results.take(1)


# Now we can use `groupByKey` to merge the User-Agent and URL tuples using the WARC-Record-ID as a key. We are also going to add two new columns for the User-Agent Family and whether it is a known bot. Some JSON files that were developed as part of the UserAgents notebook can help with this. The resulting rows will look someting like this:
# 
#     (
#         'urn:uuid:551471a6-631b-4ef7-99a5-f1344348ab64>',
#         'https://yahoo.com',
#         'Mozilla/5.0 (compatible; archive.org_bot; Wayback Machine Live Record; +http://archive.org/details/archive.org_bot)',
#       
#         'https://yahoo.com',
#         'archive.org_bot',
#         True
#     )

# In[12]:


# merge the dataset using the record-id
dataset = results.groupByKey()

# flatten the second cell into two different columns
dataset = dataset.mapValues(list)

# make sure each row has a user-agent and a url (not guaranteed)
dataset = dataset.filter(lambda r: len(r[1]) == 2)

# get our user-agent mapping dictionaries handy
import json
ua_families = json.load(open('../analysis/results/ua-families.json'))
top_uas = json.load(open('../analysis/results/top-uas.json'))

# Flatten the results so we can turn it into a DataFrame
def unpack(d):
    id = d[0]
    url, ua = d[1]
    ua_f = ua_families.get(ua, '')
    bot = top_uas.get(ua_f, False)
    return (id, url, ua, ua_f, bot)
dataset = dataset.map(unpack)

# Convert to a Spark DataFrame
df = dataset.toDF(["record_id", "url", "user_agent", "user_agent_family", "bot"])


# In[13]:


df.head(10)


# Ok let's save off these results before we do any more processing.

# In[14]:


df.write.csv('../analysis/results/urls')


# Now let's count the URLs and see which ones have appeared more than once.

# In[18]:


from pyspark.sql.functions import countDistinct, desc

url_counts = df.groupBy("url").agg(countDistinct("url").alias("count")).sort(desc("count"))
url_counts.write.csv('../analysis/results/url-counts/')


# In[ ]:




