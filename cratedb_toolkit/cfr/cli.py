# Copyright (c) 2021-2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import multiprocessing
import sys

import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.cfr.systable import Archive, SystemTableExporter, SystemTableImporter
from cratedb_toolkit.util.cli import (
    boot_click,
    error_logger,
    make_command,
)
from cratedb_toolkit.util.data import jd, path_from_url

logger = logging.getLogger(__name__)


cratedb_sqlalchemy_option = click.option(
    "--cratedb-sqlalchemy-url", envvar="CRATEDB_SQLALCHEMY_URL", type=str, required=False, help="CrateDB SQLAlchemy URL"
)


@click.group(cls=ClickAliasedGroup)  # type: ignore[arg-type]
@cratedb_sqlalchemy_option
@click.option("--verbose", is_flag=True, required=False, help="Turn on logging")
@click.option("--debug", is_flag=True, required=False, help="Turn on logging with debug level")
@click.option("--scrub", envvar="SCRUB", is_flag=True, required=False, help="Blank out identifiable information")
@click.version_option()
@click.pass_context
def cli(ctx: click.Context, cratedb_sqlalchemy_url: str, verbose: bool, debug: bool, scrub: bool):
    """
    Diagnostics and informational utilities.
    """
    if not cratedb_sqlalchemy_url:
        logger.error("Unable to operate without database address")
        sys.exit(1)
    ctx.meta.update({"cratedb_sqlalchemy_url": cratedb_sqlalchemy_url, "scrub": scrub})
    return boot_click(ctx, verbose, debug)


@make_command(cli, "sys-export")
@click.argument("target", envvar="CFR_TARGET", type=str, required=False, default="file://./cfr")
@click.pass_context
def sys_export(ctx: click.Context, target: str):
    cratedb_sqlalchemy_url = ctx.meta["cratedb_sqlalchemy_url"]
    try:
        target_path = path_from_url(target)
        stc = SystemTableExporter(dburi=cratedb_sqlalchemy_url, target=target_path)

        archive = None
        if target_path.name.endswith(".tgz") or target_path.name.endswith(".tar.gz"):
            archive = Archive(stc)

        path = stc.save()

        if archive is not None:
            path = archive.make_tarfile()
            archive.close()
            logger.info(f"Created archive file {target}")

        jd({"path": str(path)})
    except Exception as ex:
        error_logger(ctx)(ex)
        sys.exit(1)


@make_command(cli, "sys-import")
@click.argument("source", envvar="CFR_SOURCE", type=str, required=True)
@click.pass_context
def sys_import(ctx: click.Context, source: str):
    cratedb_sqlalchemy_url = ctx.meta["cratedb_sqlalchemy_url"]
    try:
        stc = SystemTableImporter(dburi=cratedb_sqlalchemy_url, source=path_from_url(source))
        stc.load()
    except Exception as ex:
        error_logger(ctx)(ex)
        sys.exit(1)


if getattr(sys, "frozen", False):
    # https://github.com/pyinstaller/pyinstaller/issues/6368
    multiprocessing.freeze_support()
    # https://stackoverflow.com/questions/45090083/freeze-a-program-created-with-pythons-click-pacage
    cli(sys.argv[1:])
