#!/usr/bin/env python

import re
import hashlib

from os import listdir
from os.path import dirname, join, isfile
import internetarchive as ia

data_dir = join(dirname(__file__), '..', 'data')

def main():
    for item_id in listdir(data_dir):
        check(item_id)

def check(item_id):
    print(item_id)
    item = ia.get_item(item_id)
    for file in item.item_metadata.get('files', []):
        path = join(data_dir, item_id, file['name'])
        if re.search('((arc)|(cdx)).gz$', path):
            if not isfile(path):
                print("ERROR missing file {}".format(path))
                continue
            sha1 = get_sha1(path)
            if sha1 != file['sha1']:
                print("ERROR: {} {} {}".format(path, file['sha1'], sha1))


def get_sha1(path):
    fh = open(path, 'rb')
    h = hashlib.sha1()
    for chunk in iter(lambda: fh.read(2 ** 20), b""):
        h.update(chunk)
    return h.hexdigest()

if __name__ == "__main__":
    main()
