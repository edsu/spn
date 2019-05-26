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
            yield ua


# Let's create a function that will run a Spark job to get the User-Agent counts for a year as a dictionary.

# In[22]:


import pandas

def get_year(year):
    warc_files = glob.glob('warcs/liveweb-{}*/*.warc.gz'.format(year))
    warcs = sc.parallelize(warc_files)
    output = warcs.mapPartitions(ua)
    return output.countByValue()


# Now we can use `get_year` for each year of data we have for SPN and create a pandas DataFrame of the results.

# In[ ]:


ua_data = {}
for year in range(2013, 2019):
    print(year)
    ua_data[str(year)] = get_year(year)

df = pandas.DataFrame(ua_data)
df.index.name = 'ua'


# In[ ]:


df = df.sort_values(by='2018', ascending=False)
print(df.head())


# In[ ]:


df.to_csv('../analysis/results/ua.csv')


# In[ ]:




