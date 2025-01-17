# Copyright (c) 2021-2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import multiprocessing
import os
import sys
import typing as t

import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.api.cli import make_cli
from cratedb_toolkit.cfr.info import InfoRecorder
from cratedb_toolkit.cfr.systable import Archive, SystemTableExporter, SystemTableImporter
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util import DatabaseAdapter
from cratedb_toolkit.util.cli import docstring_format_verbatim, error_logger, make_command
from cratedb_toolkit.util.data import jd, path_from_url

logger = logging.getLogger(__name__)

cli = make_cli()


@make_command(cli, "sys-export")
@click.argument("target", envvar="CFR_TARGET", type=str, required=False, default="file://./cfr")
@click.pass_context
def sys_export(ctx: click.Context, target: str):
    """
    Export CrateDB system tables.
    """
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
    """
    Import CrateDB system tables.
    """
    cratedb_sqlalchemy_url = ctx.meta["cratedb_sqlalchemy_url"]
    try:
        stc = SystemTableImporter(dburi=cratedb_sqlalchemy_url, source=path_from_url(source))
        stc.load()
    except Exception as ex:
        error_logger(ctx)(ex)
        sys.exit(1)


def help_statistics():
    """
    Database cluster job / query statistics.

    Synopsis
    ========

    export CRATEDB_SQLALCHEMY_URL=crate://localhost/
    ctk cfr jobstats collect
    ctk cfr jobstats view

    """  # noqa: E501


@click.group(cls=ClickAliasedGroup, help=docstring_format_verbatim(help_statistics.__doc__))  # type: ignore[arg-type]
@click.pass_context
def job_statistics(ctx: click.Context):
    """
    Collect and display statistics about jobs / queries.
    """
    pass


cli.add_command(job_statistics, name="jobstats")


@make_command(job_statistics, "collect", "Collect statistics about queries from sys.jobs_log.")
@click.option("--once", is_flag=True, default=False, required=False, help="Whether to record only one sample")
@click.pass_context
def job_statistics_collect(ctx: click.Context, once: bool):
    """
    Run jobs_log collector.
    """
    import cratedb_toolkit.cfr.jobstats

    address = DatabaseAddress.from_string(ctx.meta["cratedb_http_url"] or ctx.meta["cratedb_sqlalchemy_url"])

    cratedb_toolkit.cfr.jobstats.boot(address=address)
    if once:
        cratedb_toolkit.cfr.jobstats.record_once()
    else:
        cratedb_toolkit.cfr.jobstats.record_forever()


@make_command(job_statistics, "view", "View job statistics per JSON output.")
@click.pass_context
def job_statistics_view(ctx: click.Context):
    """
    View job statistics about collected queries.
    """
    import cratedb_toolkit.cfr.jobstats

    address = DatabaseAddress.from_string(ctx.meta["cratedb_http_url"] or ctx.meta["cratedb_sqlalchemy_url"])
    cratedb_toolkit.cfr.jobstats.boot(address=address)

    response: t.Dict = {"meta": {}, "data": {}}
    response["meta"]["remark"] = "WIP! This is a work in progress. The output format will change."
    response["data"]["stats"] = cratedb_toolkit.cfr.jobstats.read_stats()
    jd(response)


@make_command(job_statistics, "report", "View job statistics per report.")
@click.pass_context
def job_statistics_report(ctx: click.Context):
    """
    View job statistics about collected queries per report.
    """
    import cratedb_toolkit.cfr.marimo

    address = DatabaseAddress.from_string(ctx.meta["cratedb_http_url"] or ctx.meta["cratedb_sqlalchemy_url"])
    os.environ["CRATEDB_SQLALCHEMY_URL"] = address.dburi
    cratedb_toolkit.cfr.marimo.app.run()


@make_command(job_statistics, "ui", "View job statistics per web UI.")
@click.pass_context
def job_statistics_ui(ctx: click.Context):
    """
    View job statistics about collected queries per web UI.
    """
    import marimo
    import uvicorn
    from fastapi import FastAPI

    import cratedb_toolkit.cfr.marimo

    address = DatabaseAddress.from_string(ctx.meta["cratedb_http_url"] or ctx.meta["cratedb_sqlalchemy_url"])
    os.environ["CRATEDB_SQLALCHEMY_URL"] = address.dburi
    server = marimo.create_asgi_app()
    server = server.with_app(path="/", root=cratedb_toolkit.cfr.marimo.__file__)
    app = FastAPI()
    app.mount("/", server.build())
    uvicorn.run(app, host="localhost", port=7777, log_level="info")


@click.group(cls=ClickAliasedGroup)  # type: ignore[arg-type]
@click.pass_context
def info(ctx: click.Context):
    """
    Collect and display cluster information.
    """
    pass


cli.add_command(info, name="info")


@make_command(info, "record", "Record outcomes of `ctk info cluster` and `ctk info jobs`.")
@click.option("--once", is_flag=True, default=False, required=False, help="Whether to record only one sample")
@click.pass_context
def record(ctx: click.Context, once: bool):
    scrub = ctx.meta.get("scrub", False)
    address = DatabaseAddress.from_string(ctx.meta["cratedb_http_url"] or ctx.meta["cratedb_sqlalchemy_url"])
    adapter = DatabaseAdapter(dburi=address.dburi, echo=False)
    recorder = InfoRecorder(adapter=adapter, scrub=scrub)
    if once:
        recorder.record_once()
    else:
        recorder.record_forever()


if getattr(sys, "frozen", False):
    # https://github.com/pyinstaller/pyinstaller/issues/6368
    multiprocessing.freeze_support()
    # https://stackoverflow.com/questions/45090083/freeze-a-program-created-with-pythons-click-pacage
    cli(sys.argv[1:])
