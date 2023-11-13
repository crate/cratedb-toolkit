import typing as t
from pathlib import Path

from cr8.insert_json import insert_json


def cr8_insert_json(infile: t.Union[str, Path, t.IO[t.Any]], hosts: str, table: str):
    return insert_json(table=table, bulk_size=5_000, hosts=hosts, infile=infile, output_fmt="json")
