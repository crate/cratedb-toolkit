import json
import logging
import os
import sys
from typing import Optional

import click
from dotenv import load_dotenv

from cratedb_toolkit.query.nlsql.api import DataQuery
from cratedb_toolkit.query.nlsql.model import DatabaseInfo
from cratedb_toolkit.util.common import setup_logging
from cratedb_toolkit.util.data import asbool

logger = logging.getLogger(__name__)


def help_llm():
    """
    Use an LLM to query the database in human language.

    Synopsis
    ========

    export CRATEDB_CLUSTER_URL=crate://localhost/
    ctk query nlsql "What is the average value for sensor 1?"

    """  # noqa: E501


@click.command()
@click.argument("question")
@click.option("--schema", type=str, required=False, help="Database schema where to operate on")
@click.option("--llm-provider", type=str, required=False, help="LLM provider name")
@click.option("--llm-endpoint", type=str, required=False, help="LLM endpoint URL")
@click.option(
    "--llm-instance", type=str, required=False, help="LLM model resource name, e.g. with Azure OpenAI service"
)
@click.option("--llm-name", type=str, required=False, help="LLM model name for completions")
@click.option("--llm-api-key", type=str, required=False, help="LLM API key")
@click.option("--llm-api-version", type=str, required=False, help="LLM API version")
@click.pass_context
def llm_cli(
    ctx: click.Context,
    question: str,
    schema: Optional[str],
    llm_provider: Optional[str],
    llm_endpoint: Optional[str],
    llm_instance: Optional[str],
    llm_name: Optional[str],
    llm_api_key: Optional[str],
    llm_api_version: Optional[str],
):
    """
    Use an LLM to query a database in human language.
    """
    from cratedb_toolkit.query.nlsql.util import read_llm_options

    setup_logging()
    load_dotenv()

    # Read question.
    if question == "-":
        question = sys.stdin.read().strip()

    schema = schema or os.getenv("CRATEDB_SCHEMA") or "doc"
    permit_all_statements = asbool(os.getenv("NLSQL_PERMIT_ALL_STATEMENTS"))

    # Connect to database and configure LLM.
    dburi = ctx.meta["address"].cluster_url

    # Configure natural language query machinery.
    dataquery = DataQuery(
        db=DatabaseInfo(
            dburi=dburi,
            schema=schema,
        ),
        model=read_llm_options(
            llm_provider=llm_provider,
            llm_name=llm_name,
            llm_endpoint=llm_endpoint,
            llm_instance=llm_instance,
            llm_api_key=llm_api_key,
            llm_api_version=llm_api_version,
        ),
        permit_all_statements=permit_all_statements,
    )

    # Submit query.
    response = dataquery.ask(question)
    output = {"question": question, "answer": str(response)}
    if response.metadata:
        output.update(next(iter(response.metadata.values())))
    print(json.dumps(output, indent=2), file=sys.stdout)  # noqa: T201
