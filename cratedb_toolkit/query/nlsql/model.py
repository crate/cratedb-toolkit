import dataclasses
import os
from enum import Enum
from typing import List, Optional

import sqlalchemy as sa


class ModelProvider(Enum):
    """Model provider choices."""

    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    AZURE = "azure"
    HUGGINGFACE_API = "huggingface_api"
    MISTRAL = "mistral"
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
            elif provider in [ModelProvider.ANTHROPIC]:
                llm_name = "claude-sonnet-4-0"
            elif provider in [ModelProvider.MISTRAL]:
                llm_name = "mistral-medium-latest"
            elif provider in [ModelProvider.HUGGINGFACE_API]:
                llm_name = "HuggingFaceH4/zephyr-7b-alpha"
            else:
                raise ValueError("LLM completion model not defined")

        if provider is ModelProvider.ANTHROPIC:
            llm_api_key = llm_api_key or os.getenv("ANTHROPIC_API_KEY")
            if not llm_api_key:
                raise ValueError(
                    "LLM API key not defined. Use either CLI/API parameter or ANTHROPIC_API_KEY environment variable."
                )
        elif provider is ModelProvider.AZURE:
            llm_endpoint = llm_endpoint or os.getenv("AZURE_OPENAI_ENDPOINT")
            llm_api_key = llm_api_key or os.getenv("AZURE_OPENAI_API_KEY")
            llm_api_version = llm_api_version or os.getenv("OPENAI_API_VERSION")
            if not llm_api_key:
                raise ValueError(
                    "LLM API key not defined. Use either CLI/API parameter or "
                    "AZURE_OPENAI_API_KEY environment variable."
                )
            if not llm_endpoint:
                raise ValueError(
                    "Azure OpenAI endpoint not defined. Use either CLI/API parameter or "
                    "AZURE_OPENAI_ENDPOINT environment variable."
                )
            if not llm_api_version:
                raise ValueError(
                    "Azure OpenAI API version not defined. Use either CLI/API parameter or "
                    "OPENAI_API_VERSION environment variable."
                )
        elif provider is ModelProvider.HUGGINGFACE_API:
            llm_api_key = llm_api_key or os.getenv("HF_TOKEN")
            if not llm_api_key:
                raise ValueError(
                    "LLM API token not defined. Use either CLI/API parameter or HF_TOKEN environment variable."
                )
        elif provider is ModelProvider.MISTRAL:
            llm_api_key = llm_api_key or os.getenv("MISTRAL_API_KEY")
            if not llm_api_key:
                raise ValueError(
                    "LLM API key not defined. Use either CLI/API parameter or MISTRAL_API_KEY environment variable."
                )
        elif provider is ModelProvider.OPENAI:
            llm_api_key = llm_api_key or os.getenv("OPENAI_API_KEY")
            if not llm_api_key:
                raise ValueError(
                    "LLM API key not defined. Use either CLI/API parameter or OPENAI_API_KEY environment variable."
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
