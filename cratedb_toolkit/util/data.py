import json
import sys
import typing as t


def jd(data: t.Any):
    """
    Pretty-print JSON with indentation.
    """
    print(json.dumps(data, indent=2), file=sys.stdout)  # noqa: T201


def str_contains(haystack, *needles):
    """
    Whether haystack contains any of the provided needles.
    """
    haystack = str(haystack)
    return any(needle in haystack for needle in needles)


# from sqlalchemy.util.langhelpers
# from paste.deploy.converters
def asbool(obj: t.Any) -> bool:
    if isinstance(obj, str):
        obj = obj.strip().lower()
        if obj in ["true", "yes", "on", "y", "t", "1"]:
            return True
        elif obj in ["false", "no", "off", "n", "f", "0"]:
            return False
        else:
            raise ValueError("String is not true/false: %r" % obj)
    return bool(obj)
