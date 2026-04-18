import llama_index.core
from llama_index.core.llms import LLM
from llama_index.llms.azure_openai import AzureOpenAI
from llama_index.llms.ollama import Ollama
from llama_index.llms.openai import OpenAI

from cratedb_toolkit.query.nlsql.model import ModelInfo, ModelProvider


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
    else:
        raise ValueError(f"LLM model provider not found: {info.provider}")

    return llm
