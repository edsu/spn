import warcio

def extractor(f):
    """
    A decorator for for functions that need to extract data from WARC records.
    See Spark.ipynb also in this directory for more.
    """
    def new_f(warc_files):
        for warc_file in warc_files:
            with open(warc_file, 'rb') as stream:
                for record in warcio.ArchiveIterator(stream):
                    yield from f(record)
    return new_f
