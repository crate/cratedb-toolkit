# Text-to-SQL query adapter

## Install

```shell
uv pip install 'cratedb-toolkit[nlsql]'
```

## Usage

### CLI

```shell
export CRATEDB_CLUSTER_URL=crate://localhost/
export LLM_PROVIDER=openai
export OPENAI_API_KEY=<YOUR_OPENAI_API_KEY>
```

```shell
export CRATEDB_CLUSTER_URL=crate://localhost/
export LLM_PROVIDER=anthropic
export ANTHROPIC_API_KEY=<YOUR_ANTHROPIC_API_KEY>
```

```shell
export CRATEDB_CLUSTER_URL=crate://localhost/
export LLM_PROVIDER=google
export GOOGLE_API_KEY=<YOUR_GOOGLE_API_KEY>
```

```shell
export CRATEDB_CLUSTER_URL=crate://localhost/
export LLM_PROVIDER=huggingface_api
export HUGGING_FACE_TOKEN=<YOUR_HUGGINGFACE_API_TOKEN>
```

```shell
export CRATEDB_CLUSTER_URL=crate://localhost/
export LLM_PROVIDER=mistral
export MISTRAL_API_KEY=<YOUR_MISTRAL_API_KEY>
```

```shell
export CRATEDB_CLUSTER_URL=crate://localhost/
export LLM_PROVIDER=ollama
export LLM_ENDPOINT="http://100.83.17.54:11434/"
```
```shell
ollama pull gemma3:270m  # 290 MB
ollama pull gemma3:1b    # 820 MB
ollama pull llama3.2:1b  # 1.3 GB
ollama pull qwen2.5:0.5b # 400 MB
ollama pull qwen3:0.6b   # 520 MB
```

### API

```python
import sqlalchemy as sa
from cratedb_toolkit.query.nlsql.api import DataQuery
from cratedb_toolkit.query.nlsql.model import DatabaseInfo, ModelInfo, ModelProvider

engine = sa.create_engine("crate://")
schema = "doc"

# Use Open AI GPT-4.
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.OPENAI, name="gpt-4.1"),
)

# Use Anthropic Claude Sonnet.
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.ANTHROPIC, name="claude-sonnet-4-0"),
)

# Use Google Gemini.
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.GOOGLE, name="gemini-2.5-flash"),
)

# Use Google Gemma3 via Ollama.
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.OLLAMA, name="gemma3:1b"),
)

# Use Zephyr via Hugging Face API.
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.HUGGINGFACE_API, name="zephyr-7b-alpha"),
)

# Use Mistral Medium.
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.MISTRAL, name="mistral-medium-latest"),
)

response = dataquery.ask("What is the average value for sensor 1?")
print(response)
```

## Example

```sql
CREATE TABLE IF NOT EXISTS time_series_data (
    timestamp TIMESTAMP,
    value DOUBLE,
    location STRING,
    sensor_id INT
);

INSERT INTO time_series_data (timestamp, value, location, sensor_id)
VALUES
    ('2023-09-14T00:00:00', 10.5, 'Sensor A', 1),
    ('2023-09-14T01:00:00', 15.2, 'Sensor A', 1),
    ('2023-09-14T02:00:00', 18.9, 'Sensor A', 1),
    ('2023-09-14T03:00:00', 12.7, 'Sensor B', 2),
    ('2023-09-14T04:00:00', 17.3, 'Sensor B', 2),
    ('2023-09-14T05:00:00', 20.1, 'Sensor B', 2),
    ('2023-09-14T06:00:00', 22.5, 'Sensor A', 1),
    ('2023-09-14T07:00:00', 18.3, 'Sensor A', 1),
    ('2023-09-14T08:00:00', 16.8, 'Sensor A', 1),
    ('2023-09-14T09:00:00', 14.6, 'Sensor B', 2),
    ('2023-09-14T10:00:00', 13.2, 'Sensor B', 2),
    ('2023-09-14T11:00:00', 11.7, 'Sensor B', 2);

REFRESH TABLE time_series_data;
```
```shell
ctk query nlsql "What is the average value for sensor 1?"
```
```text
Answer: The average value for sensor 1 is approximately 17.03.
```

## Local inference

:Llama-3.2-1B-Instruct: License LLaMA 3.2, Size 1.1 GB
:Qwen3.5-0.8B: License Apache 2.0, Size 1.6 GB

## Backlog

LlamaIndex provides access to many LLM models via Python packages available
on PyPI prefixed with `llama-index-llms-`.

Inference: anyscale,llamafile,localai,mistral-rs,openllm,rapid-mlx,vllm
API I: databricks,deepseek,huggingface,ibm,litellm,llama-api,llama-cpp,openai-like
API II: azure-inference,cortex,google-genai,grok,groq,meta,minimax,mlx,octoai,perplexity
Router: bedrock,bedrock-converse,cloudflare-ai-gateway,featherlessai,modelscope,nano-gpt,neutrino,openrouter,ovhcloud,rungpt
More: Dolly, Pythia, Nano-GPT (litellm), DuckDB-NSQL, nsql-llama-2-7B, pip-sql-1.3b-GGUF, SQLCoder-7B, Ellbendls/Qwen-3-4b-Text_to_SQL-GGUF
