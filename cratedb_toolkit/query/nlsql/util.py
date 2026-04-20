import contextlib
from typing import Optional

from llama_index.core import MockEmbedding, set_global_handler, settings
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.callbacks import CallbackManager
from llama_index.core.embeddings import utils
from llama_index.core.embeddings.utils import EmbedType
from llama_index.core.llms import LLM
from llama_index.llms.anthropic import Anthropic
from llama_index.llms.azure_openai import AzureOpenAI
from llama_index.llms.bedrock import Bedrock
from llama_index.llms.bedrock_converse import BedrockConverse
from llama_index.llms.huggingface_api import HuggingFaceInferenceAPI
from llama_index.llms.llamafile import Llamafile
from llama_index.llms.mistralai import MistralAI
from llama_index.llms.ollama import Ollama
from llama_index.llms.openai import OpenAI

from cratedb_toolkit.query.nlsql.model import ModelInfo, ModelProvider


def _mock_embed_model(
    embed_model: Optional[EmbedType] = None,
    callback_manager: Optional[CallbackManager] = None,
) -> BaseEmbedding:
    """Stub that suppresses embedding resolution without print/side effects."""
    return MockEmbedding(embed_dim=1)


@contextlib.contextmanager
def disable_embeddings():
    """
    Temporarily suppress LlamaIndex's embedding resolver.

    ``NLSQLTableQueryEngine`` does not require embeddings, but LlamaIndex may
    still invoke ``resolve_embed_model`` during construction.  This context
    manager replaces both resolution hooks with a no-op stub and guarantees
    the originals are restored on exit, even if an exception is raised.
    """
    original_utils = utils.resolve_embed_model
    original_settings = settings.resolve_embed_model
    try:
        utils.resolve_embed_model = _mock_embed_model  # ty: ignore[invalid-assignment]
        settings.resolve_embed_model = _mock_embed_model  # ty: ignore[invalid-assignment]
        yield
    finally:
        utils.resolve_embed_model = original_utils  # ty: ignore[invalid-assignment]
        settings.resolve_embed_model = original_settings  # ty: ignore[invalid-assignment]


def configure_llm(info: ModelInfo, debug: bool = False) -> LLM:
    """
    Configure LLM access and model types. Use either vanilla Open AI, Azure Open AI, or Ollama.

    TODO: What about Hugging Face, Runpod, vLLM, and others?
    """

    completion_model = info.name

    if not info.provider:
        raise ValueError("LLM model provider not defined")
    if not completion_model:
        raise ValueError("LLM model name not defined")

    # https://docs.llamaindex.ai/en/stable/understanding/tracing_and_debugging/tracing_and_debugging/
    if debug:
        set_global_handler("simple")

    # Select completions model.
    if info.provider is ModelProvider.OPENAI:
        llm = OpenAI(
            model=completion_model,
            temperature=0.0,
            api_key=info.api_key,
            api_version=info.api_version,
        )
    elif info.provider is ModelProvider.AZURE:
        if not info.instance:
            raise ValueError("Azure OpenAI deployment/engine instance name not defined")
        llm = AzureOpenAI(
            model=completion_model,
            temperature=0.0,
            engine=info.instance,
            azure_endpoint=info.endpoint,
            api_key=info.api_key,
            api_version=info.api_version,
        )
    elif info.provider is ModelProvider.OLLAMA:
        # https://docs.llamaindex.ai/en/stable/api_reference/llms/ollama/
        llm = Ollama(
            base_url=info.endpoint or "http://localhost:11434",
            model=completion_model,
            temperature=0.0,
            request_timeout=120.0,
            keep_alive=-1,
        )
    elif info.provider is ModelProvider.AMAZON_BEDROCK:
        from llama_index.llms.bedrock_converse.utils import bedrock_modelname_to_context_size

        llm = Bedrock(
            model=completion_model,
            temperature=0.0,
            context_size=bedrock_modelname_to_context_size(completion_model),
        )
    elif info.provider is ModelProvider.AMAZON_BEDROCK_CONVERSE:
        llm = BedrockConverse(
            model=completion_model,
            temperature=0.0,
            region_name="us-east-1",
        )
    elif info.provider is ModelProvider.ANTHROPIC:
        llm = Anthropic(
            model=completion_model,
            temperature=0.0,
            base_url=info.endpoint,
            api_key=info.api_key,
        )
    elif info.provider is ModelProvider.GOOGLE:
        from llama_index.llms.gemini import Gemini

        llm = Gemini(
            model=completion_model,
            temperature=0.0,
            base_url=info.endpoint,
            api_key=info.api_key,
        )
    elif info.provider is ModelProvider.HUGGINGFACE_API:
        llm = HuggingFaceInferenceAPI(
            model=completion_model,
            temperature=0.1,
            base_url=info.endpoint,
            token=info.api_key,
        )
    elif info.provider is ModelProvider.LLAMAFILE:
        llm = Llamafile(
            base_url=info.endpoint or "http://localhost:8080",
            temperature=0.0,
        )
    elif info.provider is ModelProvider.MISTRAL:
        llm = MistralAI(
            model=completion_model,
            temperature=0.0,
            endpoint=info.endpoint,
            api_key=info.api_key,
        )
    else:
        raise ValueError(f"LLM model provider not implemented: {info.provider}")

    return llm
