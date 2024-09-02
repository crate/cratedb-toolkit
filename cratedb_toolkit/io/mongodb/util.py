import re
import typing as t

from cratedb_toolkit.util.data_dict import OrderedDictX


def parse_input_numbers(s: str):
    """
    Parse an input string for numbers and ranges.

    Supports strings like '0 1 2', '0, 1, 2' as well as ranges such as
    '0-2'.
    """

    options: list = []
    for option in re.split(", | ", s):
        match = re.search(r"(\d+)-(\d+)", option)
        if match:
            lower, upper = sorted([match.group(1), match.group(2)])
            options = options + list(range(int(lower), int(upper) + 1))
        else:
            try:
                options.append(int(option))
            except ValueError:
                pass
    return options


def sanitize_field_names(data: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
    """
    Rename top-level column names with single leading underscores to double leading underscores.
    CrateDB does not accept singe leading underscores, like `_id`.

    This utility function to rename all relevant column names keeps their order intact.
    When loosing order is acceptable, a more efficient variant could be used.
    """
    d = OrderedDictX(data)
    for name in d.keys():
        if name.startswith("_") and not name.startswith("__"):
            d.rename_key(name, f"_{name}")
    return d
