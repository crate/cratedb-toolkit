(nlsql)=

# Natural language (NLSQL)

:::{div} sd-text-muted
Talk to your data in natural language.
:::

The CrateDB NLSQL package helps agents turn natural language into database queries,
like [Vanna AI] or Google's [QueryData] but tailored to CrateDB.

## About

NLSQL provides a straightforward way to turn natural language into executable
SQL by combining an LLM with explicit database context. It positions itself as
an execution layer for data agents: agents handle reasoning and orchestration,
while the NLSQL layer reliably generates, checks, and runs SQL against
databases, returning results for downstream actions.

The trade-off is explicit: you shift effort from prompt tuning to context
engineering and maintenance, but gain near-100% accuracy, stronger guardrails,
and production reliability—especially for multistep or mission-critical
workflows where probabilistic errors are unacceptable.

## Install

```shell
uv pip install --upgrade 'cratedb-toolkit[nlsql]'
```

## Synopsis

```shell
ctk query nlsql \
    --cluster-url="crate://crate@localhost:4200/?ssl=false" \
    --llm-provider="<provider-name>" \
    --llm-model="<model-name>" \
    --llm-api-key="<your-api-key>" \
    "What is the average value for sensor 1?"
```

## Coverage

:::{rubric} Providers
:::

Supports a range of providers
Amazon Bedrock (+ Converse), Anthropic, Azure OpenAI, Google AI,
Hugging Face Inference API, Llamafile, Mistral, Ollama, OpenAI,
OpenRouter, RunGPT, or Runpod Serverless (OpenAI-compatible).

:::{rubric} Models
:::

A wide range of models can be selected from the enumerated providers.
We recommend Gemini, Gemma3, Llama 3.1, Qwen 2.5, or later,
for example Gemma-3-1B, Llama-3.2-1B-Instruct, or Qwen3.5-0.8B.

:::{rubric} Multiple languages
:::

> Q: What is the average value for sensor 1?
>
> A: The average value for sensor 1 is approximately 17.03.

> Q: ¿Cuál es el valor medio del sensor 1?
>
> A: El valor medio del sensor 1 es 17.0333.

> Q: Quelle est la valeur moyenne du capteur 1 ?
>
> A: La valeur moyenne du capteur 1 est de 17,0333.

> Q: Wie lautet der Durchschnittswert für Sensor 1?
>
> A: Der Durchschnittswert für Sensor 1 beträgt 17,0333.

> Q: Qual è il valore medio del sensore 1?
>
> A: Il valore medio del sensore 1 è pari a 17,0333.

## Details

NLSQL works by wrapping a SQL database and exposing a query interface where
plain-language questions are translated into SQL, executed, and returned as
answers. Developers configure the engine with a database connection and a
bounded set of tables, ensuring the model generates queries only within a
known schema and avoids context overflow.

The procedure follows a schema-grounded approach: the engine injects table
structure (and optionally examples or retrieved context) into the prompt so
the LLM can synthesize accurate queries instead of guessing. It can also
integrate with retrieval components to dynamically select relevant tables
or augment prompts at query time for more complex setups.

The engine acts as a thin orchestration layer for Text-to-SQL purposes,
and for building NLSQL systems:
it handles prompt construction, query generation, execution,
and result formatting, while leaving control, safety (e.g., read-only
roles), and schema design to the developer.

## Security

Any Text-to-SQL application should be aware that executing
arbitrary SQL queries can be a security risk. It is recommended to
take precautions as needed, such as using restricted roles, read-only
databases, sandboxing, etc.

While we recommend to use a dedicated read-only user/role to guarantee
100% safety, CrateDB NLSQL also prevents [Prompt-to-SQL Injections] by
default, by classifying the SQL statement and only permitting access
for `SELECT` statements.

The `permit_all_statements` API argument or the `NLSQL_PERMIT_ALL_STATEMENTS`
environment variable can be used to relax that default when set to a boolean
value, to allow all types of statements. Only enable this flag when you are
sure about this behaviour.

## Usage

You can use CrateDB NLSQL from the command line and as a Python API.

### CLI

When using `ctk query nlsql` on the command line, we recommend to use
environment variables to configure database and LLM connectivity.

For connecting to CrateDB on localhost, use a connection string like this:
```shell
export CRATEDB_CLUSTER_URL="crate://crate:crate@localhost:4200/?ssl=false"
```
For connecting to CrateDB Cloud, use a connection string like this:
```shell
export CRATEDB_CLUSTER_URL="crate://admin:dZ...6LqB@example.eks1.eu-west-1.aws.cratedb.net:4200/?ssl=true"
```

Configure LLM provider. Use one of amazon_bedrock, amazon_bedrock_converse,
anthropic, azure, google, huggingface_api, llamafile, mistral, ollama,
openai, openrouter, rungpt, runpod_serverless.
```shell
export LLM_PROVIDER="openai"
```
Configure LLM model. The label format depends on the provider's conventions.
It is an optional configuration setting: By default, each provider will
select a standard model that is suitable for Text-to-SQL, yet cost-effective.
```shell
export LLM_NAME="google/gemma-3-4b-it:free"
```

To authenticate with LLM APIs, use corresponding environment variables like
outlined below.
```shell
export OPENAI_API_KEY="<YOUR_OPENAI_API_KEY>"
```
```shell
export ANTHROPIC_API_KEY="<YOUR_ANTHROPIC_API_KEY>"
```
```shell
export GOOGLE_API_KEY="<YOUR_GOOGLE_API_KEY>"
```
```shell
export HF_TOKEN="<YOUR_HUGGINGFACE_API_TOKEN>"
```
```shell
export MISTRAL_API_KEY="<YOUR_MISTRAL_API_KEY>"
```

(llm-ollama)=

:::{rubric} Ollama
:::

For connecting to dedicated LLM instances, use the `LLM_ENDPOINT` environment
variable. For example, to connect to a self-managed Ollama instance:
```shell
export LLM_PROVIDER="ollama"
export LLM_ENDPOINT="http://100.83.17.54:11434/"
```
```shell
ollama pull gemma3:270m  # 290 MB
ollama pull gemma3:1b    # 820 MB
ollama pull llama3.2:1b  # 1.3 GB
ollama pull qwen2.5:0.5b # 400 MB
ollama pull qwen3:0.6b   # 520 MB
```

```shell
export LLM_PROVIDER=openrouter
export LLM_NAME=google/gemma-3-4b-it:free
# Alternative model names:
# google/gemma-3n-e2b-it:free
# google/gemini-2.0-flash-lite-001
# google/gemini-2.5-flash-lite
# gryphe/mythomax-l2-13b
# ibm-granite/granite-4.0-h-micro
# liquid/lfm-2.5-1.2b-instruct:free
# meta-llama/llama-3.2-3b-instruct
# mistralai/mistral-nemo
# mistralai/mistral-small-24b-instruct-2501
# openai/gpt-oss-20b:free
# openai/gpt-oss-120b:free
export OPENROUTER_API_KEY=<YOUR_OPENROUTER_API_KEY>
```

```shell
export CRATEDB_CLUSTER_URL=crate://localhost/
export LLM_PROVIDER=runpod_serverless
export LLM_ENDPOINT="https://api.runpod.ai/v2/<YOUR_RUNPOD_ENDPOINT_ID>/openai/v1"
export RUNPOD_API_KEY=<YOUR_RUNPOD_API_KEY>
```

```shell
export CRATEDB_CLUSTER_URL=crate://localhost:4200/
export LLM_PROVIDER=llamafile
export LLM_ENDPOINT=http://localhost:8080/
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

```shell
uvx --python=3.11 --with="setuptools<82" --with="numpy<2" rungpt serve stabilityai/stablelm-tuned-alpha-3b --precision fp16 --device_map balanced
```


### API

```python
import sqlalchemy as sa
from cratedb_toolkit.query.nlsql.api import DataQuery
from cratedb_toolkit.query.nlsql.model import DatabaseInfo, ModelInfo, ModelProvider

engine = sa.create_engine("crate://")
schema = "doc"

# Use OpenAI GPT-4.
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.OPENAI, name="gpt-4.1"),
)

# Use Amazon Nova on Bedrock Converse.
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.AMAZON_BEDROCK_CONVERSE, name="global.amazon.nova-2-lite-v1:0"),
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
    model=ModelInfo(provider=ModelProvider.HUGGINGFACE_API, name="HuggingFaceH4/zephyr-7b-alpha"),
)

# Use Mistral Medium.
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.MISTRAL, name="mistral-medium-latest"),
)

# Use Gemma3 on Runpod serverless.
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(
        provider=ModelProvider.RUNPOD_SERVERLESS,
        name="gemma3:270m",
        endpoint="https://api.runpod.ai/v2/<YOUR_RUNPOD_ENDPOINT_ID>/openai/v1",
        api_key="<YOUR_RUNPOD_API_KEY>",
    ),
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


[Prompt-to-SQL Injections]: https://syssec.dpss.inesc-id.pt/papers/pedro_icse25.pdf
[QueryData]: https://cloud.google.com/blog/products/databases/introducing-querydata-for-near-100-percent-accurate-data-agents
[Vanna AI]: https://vanna.ai/
