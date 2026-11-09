import orjsonl
from fsspec import filesystem, get_filesystem_class
from fsspec.config import apply_config
from upath import UPath

# Redirects from services like GitHub can carry large CSP headers, past aiohttp's 8190-byte default.
HTTP_MAX_FIELD_SIZE = 64 * 1024


def read_json(url: str):
    """
    Read JSON file from anywhere.
    TODO: Does the resource need to be closed? How?
    """
    p = UPath(url)
    storage_options = dict(p.storage_options)
    if p.protocol in ("http", "https"):
        storage_options = apply_config(get_filesystem_class(p.protocol), storage_options)
        client_kwargs = storage_options.get("client_kwargs") or {}
        storage_options.setdefault("max_field_size", client_kwargs.get("max_field_size", HTTP_MAX_FIELD_SIZE))
    fs = filesystem(p.protocol, **storage_options)
    fp = fs.open(p.path)
    data = orjsonl.stream(fp)
    return data
