import datetime as dt
import math
import re

# Duration pattern: e.g. "7d", "24h", "30m", "90s", "2w", or combinations like "1d12h".
_DURATION_RE = re.compile(r"^(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$")


def truncate_milliseconds(timestamp: dt.datetime):
    """
    Downgrade Python datetime objects from microseconds to milliseconds resolution.

    Input:  2023-11-26 21:57:18.537624
    Output: 2023-11-26 21:57:18.537000
    """
    dec, integer = math.modf(timestamp.microsecond / 1000)
    return timestamp - dt.timedelta(microseconds=round(dec * 1000))


def parse_duration(value: str) -> dt.timedelta:
    """
    Parse a human-friendly duration string into a ``timedelta``.

    Accepted formats: ``2w``, ``7d``, ``24h``, ``30m``, ``90s``, or
    combinations like ``1d12h``. At least one component is required
    and the total duration must be positive.

    Raises ``ValueError`` for invalid or zero-length durations.
    """
    value = value.strip()
    if not value:
        raise ValueError(
            "Invalid duration: ''. Use a combination of Nw, Nd, Nh, Nm, Ns (e.g. '7d', '24h', '2w', '1d12h')."
        )
    match = _DURATION_RE.match(value)
    if not match:
        raise ValueError(
            f"Invalid duration: {value!r}. Use a combination of Nw, Nd, Nh, Nm, Ns (e.g. '7d', '24h', '2w', '1d12h')."
        )

    weeks = int(match.group(1) or 0)
    days = int(match.group(2) or 0)
    hours = int(match.group(3) or 0)
    minutes = int(match.group(4) or 0)
    seconds = int(match.group(5) or 0)

    td = dt.timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)
    if td.total_seconds() <= 0:
        raise ValueError(f"Duration must be positive, got: {value!r}")
    return td
