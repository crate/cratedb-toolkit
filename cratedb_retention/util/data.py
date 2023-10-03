import json
import typing as t


def jd(data: t.Any):
    """
    Pretty-print JSON with indentation.
    """
    print(json.dumps(data, indent=2))  # noqa: T201


def str_contains(haystack, *needles):
    """
    Whether haystack contains any of the provided needles.
    """
    haystack = str(haystack)
    return any(needle in haystack for needle in needles)
