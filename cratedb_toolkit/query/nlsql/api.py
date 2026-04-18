"""
Use an LLM to query a database in human language via NLSQLTableQueryEngine.
Example code using LlamaIndex with vanilla Open AI, Azure Open AI, or Ollama.
"""

import dataclasses
import logging
from typing import Optional

from cratedb_toolkit.query.nlsql.model import DatabaseInfo, ModelInfo

logger = logging.getLogger(__name__)


try:
    from llama_index.core.base.response.schema import RESPONSE_TYPE
    from llama_index.core.llms import LLM
    from llama_index.core.query_engine import NLSQLTableQueryEngine
    from llama_index.core.utilities.sql_wrapper import SQLDatabase
except ImportError:
    pass


@dataclasses.dataclass
class DataQuery:
    """
    DataQuery helps agents turn natural language into SQL queries.
    It's the little sister of Google's QueryData product. [1]

    We recommend evaluating the Text-to-SQL interface using the Gemma models if you are
    looking at non-frontier variants that need less resources for inference. However,
    depending on the complexity of your problem, you may also want to use cutting-edge
    models with your provider of choice at the cost of higher resource usage.

    Attention: Any natural language SQL table query engine and Text-to-SQL application
    should be aware that executing arbitrary SQL queries can be a security risk.
    It is recommended to take precautions as needed, such as using restricted roles,
    read-only databases, sandboxing, etc.

    [1] https://cloud.google.com/blog/products/databases/introducing-querydata-for-near-100-percent-accurate-data-agents
    [2] https://github.com/kupp0/multi-db-property-search-data-agents
    """

    db: DatabaseInfo
    model: ModelInfo
    query_engine: Optional["NLSQLTableQueryEngine"] = None

    def __post_init__(self):
        self.setup()

    def setup(self):
        """Configure database connection and query engine."""
        logger.info("Connecting to CrateDB")

        # Configure model.
        logger.info("Configuring LLM model")
        llm: LLM
        from cratedb_toolkit.query.nlsql.util import configure_llm

        llm = configure_llm(self.model)

        # Configure query engine.
        logger.info("Creating query engine")
        sql_database = SQLDatabase(
            self.db.engine,
            schema=self.db.schema,
            ignore_tables=self.db.ignore_tables,
            include_tables=self.db.include_tables,
        )
        self.query_engine = NLSQLTableQueryEngine(
            sql_database=sql_database,
            llm=llm,
        )

    def ask(self, question: str) -> "RESPONSE_TYPE":
        """Invoke an inquiry to the LLM."""
        if not self.query_engine:
            raise ValueError("Query engine not configured")
        logger.debug("Running query: %s", question)
        return self.query_engine.query(question)
