import json
import logging
import os
import sys
from typing import Optional

import click
from dotenv import load_dotenv

from cratedb_toolkit import DatabaseCluster
from cratedb_toolkit.query.nlsql.api import DataQuery
from cratedb_toolkit.query.nlsql.model import DatabaseInfo, ModelInfo, ModelProvider
from cratedb_toolkit.util.common import setup_logging

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
    setup_logging()
    load_dotenv()

    # Read question.
    if question == "-":
        question = sys.stdin.read().strip()

    schema = schema or os.getenv("CRATEDB_SCHEMA") or "doc"
    llm_provider = llm_provider or os.getenv("LLM_PROVIDER")
    llm_endpoint = llm_endpoint or os.getenv("LLM_ENDPOINT")
    llm_instance = llm_instance or os.getenv("LLM_INSTANCE")
    llm_name = llm_name or os.getenv("LLM_NAME")
    llm_api_key = llm_api_key or os.getenv("LLM_API_KEY")
    llm_api_version = llm_api_version or os.getenv("LLM_API_VERSION")
    if not llm_provider:
        raise click.UsageError("LLM provider name is required")

    # Connect to database and configure LLM.
    dc = DatabaseCluster.from_options(ctx.meta["address"])
    engine = dc.adapter.engine
    provider = ModelProvider(llm_provider)

    # Configure natural language query machinery.
    dataquery = DataQuery(
        db=DatabaseInfo(
            engine=engine,
            schema=schema,
        ),
        model=ModelInfo.from_options(
            provider=provider,
            llm_name=llm_name,
            llm_endpoint=llm_endpoint,
            llm_instance=llm_instance,
            llm_api_key=llm_api_key,
            llm_api_version=llm_api_version,
        ),
    )

    # Submit query.
    response = dataquery.ask(question)
    output = {"question": question, "answer": str(response)}
    if response.metadata:
        output.update(next(iter(response.metadata.values())))
    print(json.dumps(output, indent=2), file=sys.stdout)  # noqa: T201
