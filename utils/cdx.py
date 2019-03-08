import os
import internetarchive as ia

for item_id in os.listdir('data'):
    try:
        ia.download(
            item_id,
            glob_pattern="*cdx.gz",
            destdir="data",
            ignore_existing=True
        )
    except Exception as e:
        print('uhoh', item_id, e)

