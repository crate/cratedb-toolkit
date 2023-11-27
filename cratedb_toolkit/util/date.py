import datetime as dt
import math


def truncate_milliseconds(timestamp: dt.datetime):
    """
    Downgrade Python datetime objects from microseconds to milliseconds resolution.

    Input:  2023-11-26 21:57:18.537624
    Output: 2023-11-26 21:57:18.537000
    """
    dec, integer = math.modf(timestamp.microsecond / 1000)
    return timestamp - dt.timedelta(microseconds=round(dec * 1000))
