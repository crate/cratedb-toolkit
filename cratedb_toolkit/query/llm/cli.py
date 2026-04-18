import logging
import os
from typing import Optional

import click
from dotenv import load_dotenv

from cratedb_toolkit import DatabaseCluster
from cratedb_toolkit.query.llm.api import DataQuery
from cratedb_toolkit.query.llm.model import DatabaseInfo, ModelInfo, ModelProvider
from cratedb_toolkit.util.common import setup_logging

logger = logging.getLogger(__name__)


def help_llm():
    """
    Use an LLM to query the database in human language.

    Synopsis
    ========

    export CRATEDB_CLUSTER_URL=crate://localhost/
    ctk query llm "What is the average value for sensor 1?"

    """  # noqa: E501


@click.command()
@click.argument("question")
@click.option("--schema", type=str, required=False, help="Schema where to operate on")
@click.option("--llm-provider", type=str, required=False, help="LLM provider name")
@click.option("--llm-endpoint", type=str, required=False, help="LLM endpoint URL")
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
    llm_name: Optional[str],
    llm_api_key: Optional[str],
    llm_api_version: Optional[str],
):
    """
    Use an LLM to query a database in human language.
    """
    setup_logging()
    load_dotenv()

    schema = schema or os.getenv("CRATEDB_SCHEMA") or "doc"
    llm_provider = llm_provider or os.getenv("LLM_PROVIDER")
    llm_endpoint = llm_endpoint or os.getenv("LLM_ENDPOINT")
    llm_name = llm_name or os.getenv("LLM_NAME")
    llm_api_key = llm_api_key or os.getenv("LLM_API_KEY")
    if not llm_provider:
        raise click.UsageError("LLM provider name is required")

    # Connect to database and configure LLM.
    dc = DatabaseCluster.from_options(ctx.meta["address"])
    engine = dc.adapter.engine
    provider = ModelProvider(llm_provider)

    # Submit query.
    dq = DataQuery(
        db=DatabaseInfo(
            engine=engine,
            schema=schema,
        ),
        model=ModelInfo.from_options(
            provider=provider,
            llm_name=llm_name,
            llm_endpoint=llm_endpoint,
            llm_api_key=llm_api_key,
            llm_api_version=llm_api_version,
        ),
    )

    logger.info("Selected LLM: completion=%s", dq.model.name)

    response = dq.ask(question)

    logger.info("Query was: %s", question)
    logger.info("Answer was: %s", response)
    logger.info("More (metadata, formatted sources):")
    logger.info(response.get_formatted_sources())
    logger.info(response.metadata)
    return response
