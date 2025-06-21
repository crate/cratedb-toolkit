import logging

import click
from click_aliases import ClickAliasedGroup

from cratedb_toolkit.dms.table_mapping import TableMappingBuilder, get_table_names
from cratedb_toolkit.util.cli import make_command

logger = logging.getLogger(__name__)


def help_table_mappings():
    """
    Generate Amazon DMS table mapping rules.

    https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.html
    """


@click.group(cls=ClickAliasedGroup, help="AWS DMS utilities")  # type: ignore[arg-type]
@click.pass_context
def cli(ctx: click.Context):
    pass


@make_command(cli, "table-mappings", help_table_mappings)
@click.option("--address", "-a", required=True, help="source address")
@click.option("--schema", "-s", required=True, help="schema name")
@click.pass_context
def table_mappings(ctx: click.Context, address: str, schema: str):
    table_names = sorted(get_table_names(address, schema=schema))
    mapping_rules_json = TableMappingBuilder(schema=schema, names=table_names).build().render()
    print(mapping_rules_json)  # noqa: T201
