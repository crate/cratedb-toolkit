----
orphan: true
----

# NLSQL backlog

## Iteration +1

- More examples
  - https://huggingface.co/PipableAI/pip-sql-1.3b
  - https://motherduck.com/blog/duckdb-text2sql-llm/
  - https://huggingface.co/Ellbendls/Qwen-2.5-3b-Text_to_SQL-GGUF
  - https://github.com/distil-labs/distil-text2sql#usage-examples
  - https://app.readytensor.ai/publications/generating-sql-from-natural-language-using-llama-32-jOImvIBGCfwt

## Iteration +2

- Document `--include-tables`.
- Use as agentic tool? SKILLS.md? AGENTS.md?
- Exercise example that draws a table from database results.
- Exercise example that draws a graph from database results.
- Exercise example that uses time ranges.
- Exercise example that needs SQL JOINs.
- Exercise example that uses vector database features.
- Is the machinery using pgvector-specific prompt instructions
  that should be adjusted for CrateDB?
- Demonstrate Gemma3 on Bedrock
  - https://aws.amazon.com/bedrock/pricing/
  - https://github.com/run-llama/llama_index/pull/21380
- Extract NLSQL from LlamaIndex into nlsql2?
  - https://pypi.org/project/nlsql/
  - https://pypi.org/project/nlsql-api/

## Iteration +3

- Add providers: anyscale,openllm,vllm
- Validate providers: Azure, Google, Hugging Face, Mistral
- Tests: When using the vanilla schema `testdrive-data` with `from tests.conftest import TESTDRIVE_DATA_SCHEMA`,
  the LLM gets confused, and thinks the table is called `sensor_data`. The error message is:
  » The error indicates that the specified table, "sensor_data," is not recognized in the "testdrive-data" schema.
- How to prevent queries like `Who is Shakespeare?`?
- Maintain chat memory/context.
  https://github.com/run-llama/llama_index/discussions/11424
- https://unsloth.ai/docs/models/qwen3.5
  ```shell
  ollama run hf.co/unsloth/Qwen3.5-0.8B-GGUF:UD-Q4_K_XL
  ```

### Fine tuning
- Text2SQirreL 🐿️ : Query your data in plain English
  https://github.com/distil-labs/distil-text2sql
- https://yia333.medium.com/enhancing-text-to-sql-with-a-fine-tuned-7b-llm-for-database-interactions-fa754dc2e992
- https://www.promptlayer.com/models/pip-sql-13b-gguf/
  https://huggingface.co/PipableAI/pip-sql-1.3b
- https://huggingface.co/QuantFactory/Meta-Llama-3.1-8B-Text-to-SQL-GGUF
- https://motherduck.com/blog/duckdb-text2sql-llm/
  https://github.com/NumbersStationAI/DuckDB-NSQL
- https://huggingface.co/srujanamadiraju/nl-sql-gemma2b
- https://github.com/raghujhts13/text-to-sql
  https://huggingface.co/TheBloke/CodeLlama-7B-Instruct-GGUF
- https://app.readytensor.ai/publications/generating-sql-from-natural-language-using-llama-32-jOImvIBGCfwt
  https://huggingface.co/sai-santhosh/text-2-sql-gguf
- https://huggingface.co/Ellbendls/Qwen-3-4b-Text_to_SQL-GGUF
- https://huggingface.co/Ellbendls/Qwen-2.5-3b-Text_to_SQL-GGUF/blob/main/Qwen-2.5-3b-Text_to_SQL.gguf
- https://www.jan.ai/docs/desktop/jan-models/lucy
- More runtimes
  https://docs.docker.com/ai/model-runner/

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

## llamafile

```shell
export LLM_PROVIDER="llamafile"
export LLM_ENDPOINT="http://localhost:8080/"
export LLM_NAME="n/a"
export LLM_ENDPOINT="http://localhost:8080/"
```
```shell
wget https://huggingface.co/mozilla-ai/Llama-3.2-1B-Instruct-llamafile/resolve/main/Llama-3.2-1B-Instruct-Q6_K.llamafile
wget https://huggingface.co/mozilla-ai/llamafile_0.10.0/resolve/main/Qwen3.5-0.8B-Q8_0.llamafile
./Llama-3.2-1B-Instruct-Q6_K.llamafile
./Qwen3.5-0.8B-Q8_0.llamafile
```
```shell
wget "https://github.com/mozilla-ai/llamafile/releases/download/0.10.0/llamafile-0.10.0"
wget "https://huggingface.co/Ellbendls/Qwen-3-4b-Text_to_SQL-GGUF/resolve/main/Qwen-3-4b-Text_to_SQL-q2_k.gguf?download=true"
```

## Security

- https://github.com/rodrigo-pedro/P2SQL
