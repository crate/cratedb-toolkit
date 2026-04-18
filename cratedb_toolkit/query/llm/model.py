import dataclasses
import os
from enum import Enum
from typing import List, Optional

import sqlalchemy as sa


class ModelProvider(Enum):
    """Model provider choices."""

    OPENAI = "openai"
    AZURE = "azure"
    OLLAMA = "ollama"


@dataclasses.dataclass
class ModelInfo:
    """Information about the model."""

    provider: ModelProvider
    name: str
    endpoint: Optional[str] = None
    instance: Optional[str] = None
    api_key: Optional[str] = None
    api_version: Optional[str] = None

    @classmethod
    def from_options(
        cls,
        provider: ModelProvider,
        llm_endpoint: Optional[str],
        llm_instance: Optional[str],
        llm_name: Optional[str],
        llm_api_key: Optional[str],
        llm_api_version: Optional[str],
    ):
        """Read options and apply parameter sanity checks and heuristics."""
        if not llm_name:
            if provider in [ModelProvider.OPENAI, ModelProvider.AZURE]:
                llm_name = "gpt-4.1"
            elif provider in [ModelProvider.OLLAMA]:
                llm_name = "gemma3:1b"
            else:
                raise ValueError("LLM completion model not defined")
        if not llm_api_key:
            if provider in [ModelProvider.OPENAI, ModelProvider.AZURE]:
                llm_api_key = os.getenv("OPENAI_API_KEY")
                if not llm_api_key:
                    raise ValueError(
                        "LLM API key not defined. Use either API option or OPENAI_API_KEY environment variable."
                    )
        return cls(
            provider=provider,
            endpoint=llm_endpoint,
            instance=llm_instance,
            name=llm_name,
            api_key=llm_api_key,
            api_version=llm_api_version,
        )


@dataclasses.dataclass
class DatabaseInfo:
    """Information about the database."""

    engine: sa.engine.Engine
    schema: Optional[str] = None
    ignore_tables: Optional[List[str]] = None
    include_tables: Optional[List[str]] = None
