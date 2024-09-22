import orjsonl
from fsspec import filesystem
from upath import UPath


def read_json(url: str):
    """
    Read JSON file from anywhere.
    TODO: Does the resource need to be closed? How?
    """
    p = UPath(url)
    fs = filesystem(p.protocol, **p.storage_options)  # equivalent to p.fs
    fp = fs.open(p.path)
    data = orjsonl.stream(fp)
    return data
