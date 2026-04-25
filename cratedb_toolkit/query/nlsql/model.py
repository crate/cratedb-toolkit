import dataclasses
from enum import Enum
from typing import List, Optional, cast

import sqlalchemy as sa
import sqlalchemy.event
from sqlalchemy_cratedb.support import quote_relation_name


class ModelProvider(Enum):
    """Model provider choices."""

    AMAZON_BEDROCK = "amazon_bedrock"
    AMAZON_BEDROCK_CONVERSE = "amazon_bedrock_converse"
    ANTHROPIC = "anthropic"
    AZURE = "azure"
    GOOGLE = "google"
    HUGGINGFACE_SERVERLESS = "huggingface_serverless"
    LLAMAFILE = "llamafile"
    MISTRAL = "mistral"
    OLLAMA = "ollama"
    OPENAI = "openai"
    OPENROUTER = "openrouter"
    RUNPOD_SERVERLESS = "runpod_serverless"


@dataclasses.dataclass
class ModelInfo:
    """Information about the model."""

    provider: ModelProvider
    name: Optional[str] = None
    endpoint: Optional[str] = None
    instance: Optional[str] = None
    api_key: Optional[str] = None
    api_version: Optional[str] = None


@dataclasses.dataclass
class DatabaseInfo:
    """Information about the database."""

    dburi: Optional[str] = None
    engine: Optional[sa.engine.Engine] = None
    schema: Optional[str] = None
    ignore_tables: Optional[List[str]] = None
    include_tables: Optional[List[str]] = None
    _listener_registered: bool = dataclasses.field(default=False, init=False, repr=False)

    def setup(self):
        """Set up SQLAlchemy engine and schema."""

        if self.engine is None:
            if self.dburi is None:
                raise ValueError("Either SQLAlchemy connection URL or database engine object required")
            self.engine = sa.create_engine(self.dburi, echo=False)

        def receive_engine_connect(conn):
            """Configure search path."""
            if self.schema is not None:
                conn.execute(sa.text(f"SET search_path={quote_relation_name(self.schema)};"))
                conn.commit()

        if not self._listener_registered:
            sqlalchemy.event.listen(self.engine, "engine_connect", receive_engine_connect)
            self._listener_registered = True

    def get_engine(self) -> sa.engine.Engine:
        """Return SQLAlchemy engine object."""
        return cast(sa.engine.Engine, self.engine)
