# ty: ignore[unresolved-import]
from typing import Tuple

import llama_index.core
from langchain_openai import AzureOpenAIEmbeddings, OpenAIEmbeddings
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.llms import LLM
from llama_index.embeddings.langchain import LangchainEmbedding
from llama_index.embeddings.ollama import OllamaEmbedding
from llama_index.llms.azure_openai import AzureOpenAI
from llama_index.llms.ollama import Ollama
from llama_index.llms.openai import OpenAI

from cratedb_toolkit.query.llm.model import ModelInfo, ModelProvider


def configure_llm(info: ModelInfo, debug: bool = False) -> Tuple[LLM, BaseEmbedding]:
    """
    Configure LLM access and model types. Use either vanilla Open AI, Azure Open AI, or Ollama.

    TODO: What about Hugging Face, Runpod, vLLM, and others?

    Notes about text embedding models:

    > The new model, `text-embedding-ada-002`, replaces five separate models for text search,
    > text similarity, and code search, and outperforms our previous most capable model,
    > Davinci, at most tasks, while being priced 99.8% lower.

    - https://openai.com/index/new-and-improved-embedding-model/
    - https://community.openai.com/t/models-embedding-vs-similarity-vs-search-models/291265
    """

    completion_model = info.completion

    if not info.provider:
        raise ValueError("LLM model type not defined")
    if not completion_model:
        raise ValueError("LLM model name not defined")

    # https://docs.llamaindex.ai/en/stable/understanding/tracing_and_debugging/tracing_and_debugging/
    if debug:
        llama_index.core.set_global_handler("simple")

    # Select completions model.
    if info.provider is ModelProvider.OPENAI:
        llm = OpenAI(
            model=completion_model,
            temperature=0.0,
            api_key=info.api_key,
            api_version=info.api_version,
        )
    elif info.provider is ModelProvider.AZURE:
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
    else:
        raise ValueError("LLM model type invalid: %s", info.provider)

    # Select embeddings model.
    if info.provider is ModelProvider.OPENAI:
        embed_model = LangchainEmbedding(
            OpenAIEmbeddings(
                model=info.embedding,
                api_key=info.api_key,  # ty: ignore[unknown-argument]
            )
        )
    elif info.provider is ModelProvider.AZURE:
        embed_model = LangchainEmbedding(
            AzureOpenAIEmbeddings(
                azure_endpoint=info.endpoint,
                model=info.embedding,
                api_key=info.api_key,  # ty: ignore[unknown-argument]
                api_version=info.api_version,  # ty: ignore[unknown-argument]
            )
        )
    # https://pypi.org/project/llama-index-embeddings-ollama/
    # https://developers.llamaindex.ai/python/framework/integrations/embeddings/ollama_embedding/
    # https://developers.llamaindex.ai/typescript/framework/modules/models/embeddings/
    # Popular embedding models with Ollama: nomic-embed-text, embeddinggemma, mxbai-embed-large
    elif info.provider is ModelProvider.OLLAMA:
        embed_model = OllamaEmbedding(
            model_name=info.embedding,
            base_url=info.endpoint or "http://localhost:11434",
        )
    else:
        embed_model = None

    return llm, embed_model
