# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
CLI commands for Kinesis checkpoint maintenance.

Registered as ``ctk kinesis`` subcommand group. Does not depend on
the ``kinesis`` (async-kinesis) package, only on SQLAlchemy.
"""

import logging
import sys
import typing as t

import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.exception import CheckpointTableNotFound
from cratedb_toolkit.io.kinesis.maintenance import list_checkpoints, prune_checkpoints
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util.cli import make_command
from cratedb_toolkit.util.data import jd

logger = logging.getLogger(__name__)

schema_option: t.Any = click.option(
    "--schema",
    type=str,
    required=False,
    envvar="CRATEDB_EXT_SCHEMA",
    help="Schema where the checkpoint table lives (default: ext)",
)

namespace_option: t.Any = click.option(
    "--namespace",
    type=str,
    required=False,
    help="Filter by checkpoint namespace (stream name)",
)

dryrun_option: t.Any = click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show what would be deleted without actually deleting",
)


@click.group(cls=ClickAliasedGroup)
def cli():
    """Kinesis checkpoint management."""


@make_command(cli, "list-checkpoints", aliases=["ls"])
@click.argument("dburi", envvar="CRATEDB_URI")
@schema_option
@namespace_option
def list_checkpoints_cmd(dburi: str, schema: t.Optional[str], namespace: t.Optional[str]):
    """List checkpoint records."""
    import sqlalchemy as sa

    schema = schema or "ext"
    engine = sa.create_engine(str(DatabaseAddress.from_string(dburi).decode()[0]))
    try:
        rows = list_checkpoints(engine=engine, schema=schema, namespace=namespace)
    except CheckpointTableNotFound as ex:
        click.echo(str(ex), err=True)
        sys.exit(1)
    finally:
        engine.dispose()

    if not rows:
        click.echo("No checkpoints found.")
        return

    jd(rows)


@make_command(cli, "prune-checkpoints", aliases=["prune"])
@click.argument("dburi", envvar="CRATEDB_URI")
@click.option("--older-than", type=str, required=False, help="Age threshold, e.g. 30d, 24h, 2w")
@schema_option
@namespace_option
@click.option("--include-active", is_flag=True, default=False, help="Also delete active rows (use with care)")
@dryrun_option
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt")
def prune_checkpoints_cmd(
    dburi: str,
    older_than: t.Optional[str],
    schema: t.Optional[str],
    namespace: t.Optional[str],
    include_active: bool,
    dry_run: bool,
    yes: bool,
):
    """
    Delete checkpoint rows matching filters.

    At least one of --older-than or --namespace is required.
    By default only inactive rows (active=FALSE) are eligible.
    """
    import sqlalchemy as sa

    if not older_than and not namespace:
        raise click.UsageError("At least one of --older-than or --namespace is required")

    schema = schema or "ext"
    engine = sa.create_engine(str(DatabaseAddress.from_string(dburi).decode()[0]))
    try:
        if dry_run:
            result = prune_checkpoints(
                engine=engine,
                schema=schema,
                older_than=older_than,
                namespace=namespace,
                include_active=include_active,
                dry_run=True,
            )
            jd(result)
            return

        if not yes:
            preview = prune_checkpoints(
                engine=engine,
                schema=schema,
                older_than=older_than,
                namespace=namespace,
                include_active=include_active,
                dry_run=True,
            )
            count = preview["would_delete"]
            if count == 0:
                click.echo("No matching rows to prune.")
                return
            click.confirm(f"Delete {count} checkpoint row(s)?", abort=True)

        result = prune_checkpoints(
            engine=engine,
            schema=schema,
            older_than=older_than,
            namespace=namespace,
            include_active=include_active,
        )
        jd(result)
    except CheckpointTableNotFound as ex:
        click.echo(str(ex), err=True)
        sys.exit(1)
    except ValueError as ex:
        click.echo(f"Error: {ex}", err=True)
        sys.exit(1)
    finally:
        engine.dispose()
