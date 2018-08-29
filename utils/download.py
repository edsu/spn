#!/usr/bin/env python

import gzip
import json
import internetarchive as ia

out = open('metadata.json', 'w')

for item in ia.search_items('collection:liveweb'):
    print(item['identifier'])
    item = ia.get_item(item['identifier'])
    json.dump(item.item_metadata, out)
    out.write('\n')

