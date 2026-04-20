import dataclasses
from enum import Enum
from typing import List, Optional

import sqlalchemy as sa


class ModelProvider(Enum):
    """Model provider choices."""

    OPENAI = "openai"
    AMAZON_BEDROCK = "amazon_bedrock"
    AMAZON_BEDROCK_CONVERSE = "amazon_bedrock_converse"
    ANTHROPIC = "anthropic"
    AZURE = "azure"
    GOOGLE = "google"
    HUGGINGFACE_API = "huggingface_api"
    LLAMAFILE = "llamafile"
    MISTRAL = "mistral"
    OLLAMA = "ollama"
    RUNGPT = "rungpt"
    RUNPOD_SERVERLESS = "runpod_serverless"


@dataclasses.dataclass
class ModelInfo:
    """Information about the model."""

    provider: ModelProvider
    name: str
    endpoint: Optional[str] = None
    instance: Optional[str] = None
    api_key: Optional[str] = None
    api_version: Optional[str] = None


@dataclasses.dataclass
class DatabaseInfo:
    """Information about the database."""

    engine: sa.engine.Engine
    schema: Optional[str] = None
    ignore_tables: Optional[List[str]] = None
    include_tables: Optional[List[str]] = None
