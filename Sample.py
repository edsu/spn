
# coding: utf-8

# # Sample
# 
# This notebook can be used to sample WARC files from the liveweb collection, which corresponds to web archives created by Internet Archive's Save Page Now. This collection is ordinarily not public for privacy reasons, but Internet Archive do grant researchers access to their WARC collections on request.

# In[6]:


import os
import re
import json
import internetarchive as ia


# First we need to build up an index of items in the liveweb collection by date. There are thousands of items to look at, so we save the result as `items.json` which will be returned immediately if it is available, unless reindex is set to `True`. 

# In[ ]:


def get_index(reindex=False):
    # look for previously computed index
    if not reindex and os.path.isfile('items.json'):
        return json.load(open('Items.json'))
    
    item_index = {}
    
    for item in ia.search_items('collection:liveweb'):
        
        # get the date from the item identifier
        m = re.match('^.+-(\d\d\d\d)(\d\d)(\d\d)', item['identifier'])
        date = '%s-%s-%s' % m.groups()
        
        if date not in item_index:
            item_index[date] = []
 
        item_index[date].append(item['identifier'])

    # save the index to disk
    json.dump(item_index, open('Items.json', 'w'))
    return item_index


# Ok, let's run it ... this will take a while if `items.json` isn't already available.

# In[ ]:


item_index = get_index()


# Now we can determine what days we want to sample, and download the associated WARC and ARC files. In our case we are going to get all the data for a particular day each year. Feel free to change the day as needed.

# In[ ]:


for year in range(2011, 2019):
    date = '%s-05-28' % year
    for item in item_index[date]:
        print('downloading %s' % item)
        ia.download(
            id, 
            glob_pattern="*arc.gz",
            destdir="data",
            ignore_existing=True
        )


# The reality is that it can take weeks to sample and download, so you probably want to export this notebook as a .py file and run it on a server that stays online.
