----
orphan: true
----

# NLSQL backlog

## Iteration +1

- Document `--include-tables`.
- Use as agentic tool?
- Exercise example that draws a table from database results.
- Exercise example that draws a graph from database results.
- Exercise example that uses time ranges.
- Exercise example that needs SQL JOINs.
- Exercise example that uses vector database features.
- Is the machinery using pgvector-specific prompt instructions
  that should be adjusted for CrateDB?

## Iteration +2

- Add providers: anyscale,openllm,vllm
- Validate providers: Azure, Google, Hugging Face, Mistral, RunGPT
- Tests: When using the vanilla schema `testdrive-data` with `from tests.conftest import TESTDRIVE_DATA_SCHEMA`,
  the LLM gets confused, and thinks the table is called `sensor_data`. The error message is:
  » The error indicates that the specified table, "sensor_data," is not recognized in the "testdrive-data" schema.
- How to prevent queries like `Who is Shakespeare?`?

## Notes

LlamaIndex provides access to many LLM model inference engines and services via
Python packages available on PyPI prefixed with `llama-index-llms-`.
We've unlocked a few popular ones, but there are certainly many more.

- Inference: anyscale,localai,mistral-rs,openllm,rapid-mlx
- API I: databricks,deepseek,huggingface,ibm,litellm,llama-api,llama-cpp,openai-like
- API II: azure-inference,cortex,grok,groq,meta,minimax,mlx,octoai,perplexity
- Router: cloudflare-ai-gateway,featherlessai,modelscope,nano-gpt,neutrino,ovhcloud
- More I: Dolly, Pythia, Nano-GPT (litellm), DuckDB-NSQL, nsql-llama-2-7B, pip-sql-1.3b-GGUF, SQLCoder-7B, Ellbendls/Qwen-3-4b-Text_to_SQL-GGUF
- More II: kwaipilot/kat-coder-pro-v2, undi95/remm-slerp-l2-13b
