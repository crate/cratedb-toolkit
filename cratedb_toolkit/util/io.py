import orjsonl
from fsspec import filesystem
from upath import UPath


def read_json(url: str):
    """
    Read JSON file from anywhere.
    TODO: Does the resource need to be closed? How?
    """
    p = UPath(url)
    storage_options = dict(p.storage_options)
    if p.protocol in ("http", "https"):
        # Redirects from services like GitHub can carry large CSP headers.
        storage_options.setdefault("max_field_size", 64 * 1024)
    fs = filesystem(p.protocol, **storage_options)
    fp = fs.open(p.path)
    data = orjsonl.stream(fp)
    return data
