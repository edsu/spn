
# coding: utf-8

# # Sample
# 
# This notebook can be used to sample WARC files from the liveweb collection, which corresponds to web archives created by Internet Archive's Save Page Now. This collection is ordinarily not public for privacy reasons, but Internet Archive do grant researchers access to their WARC collections on request.

# In[3]:


import os
import re
import json
import internetarchive as ia


# It will be important to know the total size of each item we are going to download for diagnostic purposes. This can only be obtained by fetching the item metadata from the Internet Archive API. Given an `item_id` this function will return the date, and size for each item.

# In[4]:


def item_summary(item_id):
    print("summarizing %s" % item_id)
    item = ia.get_item(item_id)

    size = 0
    for file in item.item_metadata['files']:
        if file['name'].endswith('arc.gz'):
            size += int(file['size'])
            
    m = re.match('^.+-(\d\d\d\d)(\d\d)(\d\d)', item.item_metadata['metadata']['identifier'])
    date = '%s-%s-%s' % m.groups()
    
    return date, size


# Let's try out the function on a known identifier for a liveweb item:

# In[5]:


print(item_summary('liveweb-20180608000829'))


# Now we need to build up an index of items in the liveweb collection by date. There are thousands of items to look at, so we save the result as `items.json` which will be returned immediately if it is available, unless `reindex` is set to `True`.

# In[ ]:


def get_index(reindex=False):
    # look for previously computed index
    if not reindex and os.path.isfile('Sample.json'):
        return json.load(open('Sample.json'))
    
    item_index = {}
    
    for item in ia.search_items('collection:liveweb'):
        
        # get the date from the item identifier
        date, size = item_summary(item['identifier'])
        
        if date not in item_index:
            item_index[date] = []
 
        item_index[date].append({'id': item['identifier'], 'size': size})

    # save the index to disk
    json.dump(item_index, open('Sample.json', 'w'), indent=2)
    return item_index


# Ok, let's run it ... this will take a while if `Sample.json` isn't already available.

# In[ ]:


item_index = get_index()


# Now we can determine what days we want to sample, and download the associated WARC and ARC files. In our case we are going to get all the data for a particular day each year. Feel free to change the day as needed. According to Wikipedia June 6 is Tim Berners-Lee's birthday.

# In[ ]:


item_ids = []
total_size = 0
for year in range(2011, 2019):
    date = '%s-06-08' % year
    for item in item_index[date]:
        item_ids.append(item['id'])
        total_size += item['size']
        
print("There are %s Internet Archive items to download for June 6 over the past 8 years." % len(item_ids))
print("The total size will be %0.2f GB" % (total_size / 1024 / 1024 / 1024.0))


# Now let's download them.

# In[ ]:


count = 0
for item_id in item_index[date]:
    count += 1
    print('[%s/%s] downloading %s' % (1, len(item_ids), item_id))
    ia.download(
        item_id,
        glob_pattern="*arc.gz",
        destdir="data",
        ignore_existing=True
    )


# The reality is that it can take weeks (or months) to sample and download, so you probably want to export this notebook as a .py file and run it on a reliable server in a screen or tmux session:
# 
# ```
# % jupyter nbconvert --to script Sample.ipynb
# % python Sample.py
# ```
# 
