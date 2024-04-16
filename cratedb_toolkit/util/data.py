import datetime as dt
import json
import sys
import typing as t
from pathlib import Path

from yarl import URL


def jd(data: t.Any):
    """
    Pretty-print JSON with indentation.
    """
    print(json.dumps(data, indent=2, cls=JSONEncoderPlus), file=sys.stdout)  # noqa: T201


def str_contains(haystack, *needles):
    """
    Whether haystack contains any of the provided needles.
    """
    haystack = str(haystack)
    return any(needle in haystack for needle in needles)


def asbool(obj: t.Any) -> bool:
    # from sqlalchemy.util.langhelpers
    # from paste.deploy.converters
    if isinstance(obj, str):
        obj = obj.strip().lower()
        if obj in ["true", "yes", "on", "y", "t", "1"]:
            return True
        elif obj in ["false", "no", "off", "n", "f", "0"]:
            return False
        else:
            raise ValueError("String is not true/false: %r" % obj)
    return bool(obj)


class JSONEncoderPlus(json.JSONEncoder):
    """
    https://stackoverflow.com/a/27058505
    """

    def default(self, o):
        if isinstance(o, dt.datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


def path_from_url(url: str):
    url_obj = URL(url)
    path = Path((url_obj.host or "") + (url_obj.path or ""))
    return path.absolute()
