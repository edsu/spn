
# coding: utf-8

# # Internet Archive LiveWeb Data

# The Internet Archive's [Save Page Now]() writes WARC data for archived web content to a particular Internet Archive collection called [liveweb](https://archive.org/details/liveweb). This data is not available to the public, however metadata about the items in the collections is. This notebook shows how you can examine the metadata to get the number of bytes per day that have been archived by talking to the Internet Archive's API.
# 
# ## Install
# 
# If you are reading this you may have Jupyter already set up, but just in case you don't you'll want to install Python 3 and:
# 
#     % pip install pipenv
#     % git clone https://github.com/edsu/spn
#     % cd spn
#     % pipenv install 
#     
# And now you'll need to save your Internet Archive account login details. If you don't have an account go over to archive.org and create one, and then:
# 
#     % ia configure
#     
# ## Fetch Data
# 
# Now we're ready to load and use the [internetarchive](https://github.com/jjjake/internetarchive) Python extension:
#     

# In[17]:


import os
import re
import json

from internetarchive import search_items, get_item


# Now iterate through each item in the liveweb collection and count how much data there was per day of WARC or ARC data. This will store the dates and sizes in a dictionary keyed by date.
# 
# Note, we only need to do this if we haven't previously generated the CSV. It takes a while...

# In[22]:


sizes = {}

if not os.path.isfile('Sizes.csv'):

    for result in search_items('collection:liveweb'):
        item = get_item(result['identifier'])
    
        # count sizes of all arc.gz and warc.gz files in this item
        size = 0
        for file in item.item_metadata['files']:
            if file['name'].endswith('arc.gz'):
                size += int(file['size'])
    
        # get the date
        m = re.match('^.+-(\d\d\d\d)(\d\d)(\d\d)', item.item_metadata['metadata']['identifier'])
        date = '%s-%s-%s' % m.groups()
    
        # add this size to whatever we currently have for the date
        sizes[date] = sizes.get(date, 0) + size
    
        if len(sizes) > 50:
            break


# ## Save Data
# 
# Now let's write out the sizes by date to a CSV file. This way we won't need to fetch it every time we run the Notebook.

# In[24]:


if sizes:

    import csv

    dates = sorted(sizes.keys())

    with open('Sizes.csv', 'w') as output:
        writer = csv.writer(output)
        writer.writerow(['date', 'size'])
        for date in dates:
            writer.writerow([date, sizes[date]])

