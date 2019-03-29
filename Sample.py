
# coding: utf-8

# # Sample
# 
# This notebook can be used to sample WARC files from the liveweb collection, which corresponds to web archives created by Internet Archive's Save Page Now. This collection is ordinarily not public for privacy reasons, but Internet Archive do grant researchers access to their WARC collections on request.

# In[1]:


import os
import re
import json
import internetarchive as ia


# It will be important to know the total size of each item we are going to download for diagnostic purposes. This can only be obtained by fetching the item metadata from the Internet Archive API. Given an `item_id` this function will return the date, and size for each item.

# In[2]:


def item_summary(item_id):
    print("summarizing %s" % item_id)
    item = ia.get_item(item_id)

    size = 0
    for file in item.item_metadata.get('files', []):
        if file['name'].endswith('arc.gz'):
            size += int(file['size'])
            
    m = re.match('^.+-(\d\d\d\d)(\d\d)(\d\d)', item.item_metadata['metadata']['identifier'])
    date = '%s-%s-%s' % m.groups()
    
    return date, size


# Let's try out the function on a known identifier for a liveweb item:

# In[3]:


print(item_summary('liveweb-20180608000829'))


# Now we need to build up an index of items in the liveweb collection by date. There are thousands of items to look at, so we save the result as `items.json` which will be returned immediately if it is available, unless `reindex` is set to `True`.

# In[4]:


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

# In[5]:


item_index = get_index()


# Now we can determine what days we want to sample, and download the associated WARC and CDX files. In our case we are going to get all the data for a particular day each year. We chose **October 25, 2013** because it is our best estimate of when the SPN feature went live for the public. This is based on a few pieces of information, notably:
# 
# * the introduction of `'liveweb-` prefixed identifiers in the liveweb collection which happened on 2013-10-22. Previously only `live-` identifiers were used.
# * the only items being generated now in the liveweb collection have the `liveweb-` prefix.
# * the overwhelming majority of the items use the `liveweb-` identifier prefix: 28257 / 34336 = 82%. 
# * a conversation in Internet Archive Slack with Mark Graham and Kenji Nagahashi about how the `liveweb-` items are what SPN creates, and that the previous two years of material were of unknown origin. Or at least Kenji couldn't remember what their provenance was.
# * the record in the wayback machine: [2013-10-24](https://web.archive.org/web/20131024095443/https://archive.org/web/) and [2013-10-25](https://web.archive.org/web/20131025143028/http://archive.org/web/) seems to indicate that the SPN feature went live on 2013-10-25, 5 days after the start of `liveweb-` prefixed identifiers.
# 
# This will get a list of item identifiers that match the date and the identifier prefix. It will also keep track of how much storage will be needed to donwload them.

# In[7]:


item_ids = []
total_size = 0
for year in range(2011, 2019):
    date = '%s-10-25' % year
    for item in item_index[date]:
        if item['id'].startswith('liveweb-'):
            item_ids.append(item['id'])
            total_size += item['size']
        
print("There are %s Internet Archive items to download" % len(item_ids))
print("The total size will be %0.2f GB" % (total_size / 1024 / 1024 / 1024.0))
print("And here they are")
for item_id in item_ids:
    print(item_id)


# Now let's download them.

# In[ ]:


count = 0
for item_id in item_ids:
    count += 1
    print('[%s/%s] downloading %s' % (1, len(item_ids), item_id))
    ia.download(
        item_id,
        glob_pattern=["*arc.gz", "*cdx.gz"],
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
