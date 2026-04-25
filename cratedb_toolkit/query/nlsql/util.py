import contextlib
import os
import threading
from typing import TYPE_CHECKING, Optional

from cratedb_toolkit.query.nlsql.model import ModelInfo, ModelProvider

if TYPE_CHECKING:
    from llama_index.core.base.embeddings.base import BaseEmbedding
    from llama_index.core.callbacks import CallbackManager
    from llama_index.core.embeddings.utils import EmbedType
    from llama_index.core.llms import LLM

llama_index_import_error: Optional[ImportError] = None

try:
    from llama_index.core import MockEmbedding, set_global_handler, settings
    from llama_index.core.embeddings import utils
except ImportError as exc:
    llama_index_import_error = exc


def ensure_llama_index() -> None:
    if llama_index_import_error is not None:
        raise ImportError("NLSQL support requires installing `cratedb-toolkit[nlsql]`") from llama_index_import_error


_embedding_resolver_lock = threading.RLock()


def _mock_embed_model(
    embed_model: Optional["EmbedType"] = None,
    callback_manager: Optional["CallbackManager"] = None,
) -> "BaseEmbedding":
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
    ensure_llama_index()
    with _embedding_resolver_lock:
        original_utils = utils.resolve_embed_model
        original_settings = settings.resolve_embed_model
        try:
            utils.resolve_embed_model = _mock_embed_model  # ty: ignore[invalid-assignment]
            settings.resolve_embed_model = _mock_embed_model  # ty: ignore[invalid-assignment]
            yield
        finally:
            utils.resolve_embed_model = original_utils  # ty: ignore[invalid-assignment]
            settings.resolve_embed_model = original_settings  # ty: ignore[invalid-assignment]


DEFAULT_MODEL_MAP = {
    # https://docs.aws.amazon.com/bedrock/latest/userguide/models-supported.html
    ModelProvider.AMAZON_BEDROCK: "global.anthropic.claude-haiku-4-5-20251001-v1:0",
    # ModelProvider.AMAZON_BEDROCK_CONVERSE: "amazon.nova-micro-v1:0",  # noqa: ERA001
    ModelProvider.AMAZON_BEDROCK_CONVERSE: "global.amazon.nova-2-lite-v1:0",
    # ModelProvider.AMAZON_BEDROCK_CONVERSE: "global.anthropic.claude-haiku-4-5-20251001-v1:0",  # noqa: ERA001
    ModelProvider.ANTHROPIC: "claude-haiku-4-5",
    ModelProvider.AZURE: "gpt-4.1",  # TODO: Not validated yet.
    ModelProvider.GOOGLE: "gemini-2.5-flash",  # TODO: Not validated yet.
    ModelProvider.HUGGINGFACE_SERVERLESS: "HuggingFaceH4/zephyr-7b-alpha",  # TODO: Not validated yet.
    ModelProvider.OLLAMA: "gemma3:1b",
    ModelProvider.OPENAI: "gpt-4o-mini",
    ModelProvider.OPENROUTER: "gryphe/mythomax-l2-13b",
    ModelProvider.LLAMAFILE: "n/a",  # Only one model per process.
    ModelProvider.MISTRAL: "mistral-medium-latest",  # TODO: Not validated yet.
    ModelProvider.RUNPOD_SERVERLESS: "gemma3:270m",
}


def read_llm_options(
    llm_provider: Optional[str],
    llm_endpoint: Optional[str],
    llm_instance: Optional[str],
    llm_name: Optional[str],
    llm_api_key: Optional[str],
    llm_api_version: Optional[str],
) -> ModelInfo:
    """Read options and apply parameter sanity checks and heuristics."""

    llm_provider = llm_provider or os.getenv("LLM_PROVIDER")
    llm_endpoint = llm_endpoint or os.getenv("LLM_ENDPOINT")
    llm_instance = llm_instance or os.getenv("LLM_INSTANCE")
    llm_name = llm_name or os.getenv("LLM_NAME")
    llm_api_key = llm_api_key or os.getenv("LLM_API_KEY")
    llm_api_version = llm_api_version or os.getenv("LLM_API_VERSION")
    if not llm_provider:
        raise ValueError("LLM provider name is required")

    provider = ModelProvider(llm_provider)

    if not llm_name:
        llm_name = DEFAULT_MODEL_MAP.get(provider)

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
                "LLM API key not defined. Use either CLI/API parameter or AZURE_OPENAI_API_KEY environment variable."
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
    elif provider is ModelProvider.GOOGLE:
        llm_api_key = llm_api_key or os.getenv("GOOGLE_API_KEY")
        if not llm_api_key:
            raise ValueError(
                "LLM API key not defined. Use either CLI/API parameter or GOOGLE_API_KEY environment variable."
            )
    elif provider is ModelProvider.HUGGINGFACE_SERVERLESS:
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
    elif provider is ModelProvider.OPENROUTER:
        llm_api_key = llm_api_key or os.getenv("OPENROUTER_API_KEY")
        if not llm_api_key:
            raise ValueError(
                "LLM API key not defined. Use either CLI/API parameter or OPENROUTER_API_KEY environment variable."
            )
    elif provider is ModelProvider.RUNPOD_SERVERLESS:
        llm_api_key = llm_api_key or os.getenv("RUNPOD_API_KEY")
        if not llm_api_key:
            raise ValueError(
                "LLM API key not defined. Use either CLI/API parameter or RUNPOD_API_KEY environment variable."
            )
        if not llm_endpoint:
            raise ValueError(
                "Runpod serverless endpoint not defined. "
                "Use either CLI/API parameter or LLM_ENDPOINT environment variable."
            )
    return ModelInfo(
        provider=provider,
        endpoint=llm_endpoint,
        instance=llm_instance,
        name=llm_name,
        api_key=llm_api_key,
        api_version=llm_api_version,
    )


def configure_llm(info: ModelInfo, debug: bool = False) -> "LLM":
    """
    Configure LLM inference, local or remote.

    Supports Amazon Bedrock (+ Converse), Anthropic, Azure OpenAI, Google Gemini,
    Hugging Face Inference API, Llamafile, Mistral, Ollama, OpenAI, OpenRouter,
    and Runpod Serverless (OpenAI-compatible).
    """
    ensure_llama_index()

    completion_model = info.name

    if not info.provider:
        raise ValueError("LLM model provider not defined")
    if not completion_model:
        raise ValueError("LLM model name not defined")

    # https://docs.llamaindex.ai/en/stable/understanding/tracing_and_debugging/tracing_and_debugging/
    if debug:
        set_global_handler("simple")

    # Select completions model.
    if info.provider is ModelProvider.AMAZON_BEDROCK:
        from llama_index.llms.bedrock import Bedrock
        from llama_index.llms.bedrock_converse.utils import bedrock_modelname_to_context_size

        llm = Bedrock(
            model=completion_model,
            temperature=0.0,
            context_size=bedrock_modelname_to_context_size(completion_model),
        )
    elif info.provider is ModelProvider.AMAZON_BEDROCK_CONVERSE:
        from llama_index.llms.bedrock_converse import BedrockConverse

        llm = BedrockConverse(
            model=completion_model,
            temperature=0.0,
            region_name="us-east-1",
        )
    elif info.provider is ModelProvider.ANTHROPIC:
        from llama_index.llms.anthropic import Anthropic
        from llama_index.llms.anthropic.utils import CLAUDE_MODELS

        # TODO: Add new model types to upstream `llama-index-llms-anthropic`.
        CLAUDE_MODELS.update({"claude-opus-4-7": 200000, "claude-sonnet-4-6": 1000000, "claude-haiku-4-5": 200000})

        llm = Anthropic(
            model=completion_model,
            temperature=0.0,
            base_url=info.endpoint,
            api_key=info.api_key,
        )
    elif info.provider is ModelProvider.AZURE:
        from llama_index.llms.azure_openai import AzureOpenAI

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

    elif info.provider is ModelProvider.GOOGLE:
        from llama_index.llms.google_genai import GoogleGenAI

        llm = GoogleGenAI(
            model=completion_model,
            temperature=0.0,
            base_url=info.endpoint,
            api_key=info.api_key,
        )
    elif info.provider is ModelProvider.HUGGINGFACE_SERVERLESS:
        from llama_index.llms.huggingface_api import HuggingFaceInferenceAPI

        llm = HuggingFaceInferenceAPI(
            model=completion_model,
            temperature=0.1,
            base_url=info.endpoint,
            token=info.api_key,
        )

    elif info.provider is ModelProvider.LLAMAFILE:
        from llama_index.llms.llamafile import Llamafile

        llm = Llamafile(
            base_url=info.endpoint or "http://localhost:8080",
            temperature=0.0,
        )
    elif info.provider is ModelProvider.MISTRAL:
        from llama_index.llms.mistralai import MistralAI

        llm = MistralAI(
            model=completion_model,
            temperature=0.0,
            endpoint=info.endpoint,
            api_key=info.api_key,
        )

    elif info.provider is ModelProvider.OLLAMA:
        # https://docs.llamaindex.ai/en/stable/api_reference/llms/ollama/
        from llama_index.llms.ollama import Ollama

        llm = Ollama(
            base_url=info.endpoint or "http://localhost:11434",
            model=completion_model,
            temperature=0.0,
            request_timeout=120.0,
            keep_alive=-1,
        )
    elif info.provider is ModelProvider.OPENAI:
        from llama_index.llms.openai import OpenAI

        llm = OpenAI(
            model=completion_model,
            temperature=0.0,
            api_key=info.api_key,
            api_version=info.api_version,
        )
    elif info.provider is ModelProvider.OPENROUTER:
        from llama_index.llms.openrouter.base import DEFAULT_API_BASE, DEFAULT_MODEL, OpenRouter

        llm = OpenRouter(
            model=completion_model or DEFAULT_MODEL,
            temperature=0.0,
            api_base=info.endpoint or DEFAULT_API_BASE,
            api_key=info.api_key,
        )
    elif info.provider is ModelProvider.RUNPOD_SERVERLESS:
        from llama_index.llms.openai_like import OpenAILike

        if not info.name:
            raise ValueError("LLM model name is required")
        if not info.endpoint:
            raise ValueError("Runpod serverless endpoint is required")

        llm = OpenAILike(
            model=info.name,
            temperature=0.0,
            api_base=info.endpoint,
            api_key=info.api_key,
        )
    else:
        raise ValueError(f"LLM model provider not implemented: {info.provider}")

    return llm
