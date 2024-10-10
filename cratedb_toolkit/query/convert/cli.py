import logging
import sys
from pathlib import Path

import click

from cratedb_toolkit.query.convert.basic import sql_relocate_pks_dynamodb_ctk_0_0_27
from cratedb_toolkit.util.cli import split_list

logger = logging.getLogger(__name__)


@click.command()
@click.argument("input")
@click.option("--type", "type_", type=str, required=True, help="Select converter type")
@click.option("--primary-keys", type=str, required=False, help="Define primary keys, using a comma-separated list")
@click.pass_context
def convert_query(
    ctx: click.Context,
    input: str,  # noqa: A002
    type_: str,
    primary_keys: str,
):
    """
    Query expression conversion.
    """

    if type_ == "ddb-relocate-pks":
        data = sql_relocate_pks_dynamodb_ctk_0_0_27(read_resource(input), pks=split_list(primary_keys))
        sys.stdout.write(data)
    else:
        raise ValueError(f"Unknown converter: {type_}")


def read_resource(resource: str) -> str:
    if resource == "-":
        return sys.stdin.read()

    resource_path = Path(resource)
    if resource_path.exists():
        return resource_path.read_text()

    raise IOError(f"Could not find or access resource: {resource}")
