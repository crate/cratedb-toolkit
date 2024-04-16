# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import dataclasses
import logging
import sys
import textwrap
import typing as t

import click

from cratedb_toolkit.util.common import setup_logging

logger = logging.getLogger(__name__)


def boot_click(ctx: click.Context, verbose: bool = False, debug: bool = False):
    """
    Bootstrap the CLI application.
    """

    # Adjust log level according to `verbose` / `debug` flags.
    log_level = logging.INFO
    if debug:
        log_level = logging.DEBUG

    # Setup logging, according to `verbose` / `debug` flags.
    setup_logging(level=log_level, verbose=verbose)


def split_list(value: str, delimiter: str = ",") -> t.List[str]:
    if value is None:
        return []
    return [c.strip() for c in value.split(delimiter)]


def to_list(x: t.Any, default: t.List[t.Any] = None) -> t.List[t.Any]:
    if not isinstance(default, t.List):
        raise ValueError("Default value is not a list")
    if x is None:
        return default
    if not isinstance(x, t.Iterable) or isinstance(x, str):
        return [x]
    elif isinstance(x, list):
        return x
    else:
        return list(x)


def docstring_format_verbatim(text: t.Optional[str]) -> str:
    """
    Format docstring to be displayed verbatim as a help text by Click.

    - https://click.palletsprojects.com/en/8.1.x/documentation/#preventing-rewrapping
    - https://github.com/pallets/click/issues/56
    """
    text = text or ""
    text = textwrap.dedent(text)
    lines = [line if line.strip() else "\b" for line in text.splitlines()]
    return "\n".join(lines)


def boot_with_dburi():
    """
    Obtain a single positional argument from the command line,
    the database URI, in SQLAlchemy-compatible string format.
    """
    setup_logging()
    try:
        dburi = sys.argv[1]
    except IndexError:
        dburi = "crate://localhost/"
    return dburi


def make_command(cli, name, help=None, aliases=None):  # noqa: A002
    """
    Convenience shortcut for creating a subcommand.
    """
    kwargs = {}
    if isinstance(help, str):
        kwargs["help"] = help
    elif callable(help):
        kwargs["help"] = docstring_format_verbatim(help.__doc__)
    return cli.command(
        name,
        context_settings={"max_content_width": 120},
        aliases=aliases,
        **kwargs,
    )


def click_options_from_dataclass(cls):
    """
    Generate Click command line options dynamically from dataclass definition.

    Inspired by:
    https://www.zonca.dev/posts/2022-10-26-click-commandline-class-arguments
    """

    def decorator(f):
        fields = dataclasses.fields(cls)
        for field in fields:
            if field.metadata.get("read_only", False):
                continue
            field_name = field.name.replace("_", "-")
            if field_name not in ["self"]:
                click.option("--" + field_name, required=False, type=str, help=field.metadata.get("help", ""))(f)
        return f

    return decorator


def error_level_by_debug(debug: bool):
    if debug:
        return logger.exception
    else:
        return logger.error


def running_with_debug(ctx: click.Context) -> bool:
    return (
        (ctx.parent and ctx.parent.params.get("debug", False))
        or (ctx.parent and ctx.parent.parent and ctx.parent.parent.params.get("debug", False))
        or False
    )


def error_logger(about: t.Union[click.Context, bool]) -> t.Callable:
    if isinstance(about, click.Context):
        return error_level_by_debug(running_with_debug(about))
    if isinstance(about, bool):
        return error_level_by_debug(about)
    raise TypeError(f"Unknown type for argument: {about}")
