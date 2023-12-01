# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import datetime as dt
import functools as ft
import typing as t

from boltons.iterutils import get_path

from cratedb_toolkit import __appname__, __version__


def get_baseinfo():
    data: t.Dict[str, t.Union[str, dt.datetime]] = {}
    data["system_time"] = dt.datetime.now()
    data["application_name"] = __appname__
    data["application_version"] = __version__
    return data


def get_single_value(column_name: str):
    return ft.partial(get_path, path=(0, column_name))
