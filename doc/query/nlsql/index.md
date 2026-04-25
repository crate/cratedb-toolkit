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
    --llm-name="<model-name>" \
    --llm-api-key="<your-api-key>" \
    "What is the average value for sensor 1?"
```

## Coverage

:::{rubric} Providers
:::

Supports a range of providers:
Amazon Bedrock (+ Converse), Anthropic, Azure OpenAI, Google AI,
Hugging Face Inference API, llamafile, Mistral, Ollama, OpenAI,
OpenRouter, or Runpod Serverless (OpenAI-compatible).

:::{rubric} Models
:::

A range of models can be selected from the providers enumerated above.
We recommend Gemini, Gemma3, Llama 3.1, Qwen 2.5, or later,
for example Gemma-3-1B, Llama-3.2-1B-Instruct, or Qwen3.5-0.8B.

## Details

The NLSQL interface works by wrapping a SQL database and exposing a query
interface where plain-language questions are translated into SQL, executed,
and returned as answers.
Developers configure the engine with a database connection and a
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

CrateDB NLSQL provides a command line interface and a Python API.

### CLI

When using `ctk query nlsql` on the command line, we recommend to use
environment variables to configure database and LLM connectivity.

:::{rubric} Configure database
:::

For connecting to CrateDB on localhost, use a connection string like this:
```shell
export CRATEDB_CLUSTER_URL="crate://crate:crate@localhost:4200/?ssl=false"
```

For connecting to CrateDB Cloud, use a connection string like this:
```shell
export CRATEDB_CLUSTER_URL="crate://admin:dZ...6LqB@example.eks1.eu-west-1.aws.cratedb.net:4200/?ssl=true"
```

:::{rubric} Configure LLM
:::

Configure LLM provider, model, and access credentials when applicable.
Available providers are `amazon_bedrock`, `amazon_bedrock_converse`,
`anthropic`, `azure`, `google`, `huggingface_serverless`, `llamafile`,
`mistral`, `ollama`, `openai`, `openrouter`, `runpod_serverless`.
Available models and label formats depend on the provider's conventions.
To authenticate with LLM APIs, use corresponding `*_API_KEY` environment
variables like outlined below.

```shell
export LLM_PROVIDER="<LLM_PROVIDER>"
export LLM_NAME="<LLM_NAME>"
export PROVIDER_API_KEY="<YOUR_PROVIDER_API_KEY>"
```
Note that `LLM_NAME` is an optional configuration setting: By default,
selecting a provider automatically selects a cost-effective standard
model that is suitable for Text-to-SQL.

Select and configure the LLM of your choice.

::::{tab-set}
:::{tab-item} Amazon
Use Amazon Nova on [Amazon Bedrock].
```shell
export LLM_PROVIDER="amazon_bedrock_converse"
export LLM_NAME="global.amazon.nova-2-lite-v1:0"
```
:::
:::{tab-item} Anthropic
Use [Anthropic Claude].
```shell
export LLM_PROVIDER="anthropic"
export LLM_NAME="claude-haiku-4-5"
export ANTHROPIC_API_KEY="<YOUR_ANTHROPIC_API_KEY>"
```
:::
:::{tab-item} Azure
Use GPT on [Azure OpenAI].
```shell
export LLM_PROVIDER="azure"
export LLM_NAME="gpt-4.1"
export LLM_INSTANCE="my_embedding-model"
export AZURE_OPENAI_ENDPOINT="https://acme-openai.openai.azure.com/"
export AZURE_OPENAI_API_KEY="<YOUR_AZURE_OPENAI_API_KEY>"
```
:::
:::{tab-item} Google
Use [Gemini Flash] from Google.
```shell
export LLM_PROVIDER="google"
export LLM_NAME="gemini-2.5-flash"
export GOOGLE_API_KEY="<YOUR_GOOGLE_API_KEY>"
```
:::
:::{tab-item} Hugging Face Serverless
Use Zephyr on the [Hugging Face Serverless Inference API].
```shell
export LLM_PROVIDER="huggingface_serverless"
export LLM_NAME="HuggingFaceH4/zephyr-7b-alpha"
export HF_TOKEN="<YOUR_HUGGINGFACE_API_TOKEN>"
```
:::
:::{tab-item} Mistral
Use models from [Mistral AI].
```shell
export LLM_PROVIDER="mistral"
export LLM_NAME="mistral-medium-latest"
export MISTRAL_API_KEY="<YOUR_MISTRAL_API_KEY>"
```
:::
:::{tab-item} Ollama
Use [Ollama] to run models on your own machines.

For connecting to dedicated LLM instances, use the `LLM_ENDPOINT` environment
variable. For example, to connect to a self-managed Ollama instance, configure
those environment variables:
```shell
export LLM_PROVIDER="ollama"
export LLM_ENDPOINT="http://100.83.17.54:11434/"
export LLM_NAME="gemma3:270m"
```
Before running `ctk query nlsql`, acquire models:
```shell
ollama pull gemma3:270m  # 290 MB
ollama pull gemma3:1b    # 820 MB
ollama pull llama3.2:1b  # 1.3 GB
ollama pull qwen2.5:0.5b # 400 MB
ollama pull qwen3:0.6b   # 520 MB
ollama pull hf.co/Menlo/Lucy-128k-gguf:Q4_K_M  # 1.1 GB
```
:::
:::{tab-item} OpenAI
Use [GPT‑4o mini] from [OpenAI].
```shell
export LLM_PROVIDER="openai"
export LLM_NAME="gpt-4o-mini"
export OPENAI_API_KEY="<YOUR_OPENAI_API_KEY>"
```
:::
:::{tab-item} OpenRouter
Choose from many models available via [OpenRouter].
```shell
export LLM_PROVIDER="openrouter"
export LLM_NAME="google/gemma-3-4b-it:free"
export OPENROUTER_API_KEY="<YOUR_OPENROUTER_API_KEY>"
```
Alternative model names:
```text
google/gemma-3n-e2b-it:free
google/gemini-2.0-flash-lite-001
google/gemini-2.5-flash-lite
gryphe/mythomax-l2-13b
ibm-granite/granite-4.0-h-micro
liquid/lfm-2.5-1.2b-instruct:free
meta-llama/llama-3.2-3b-instruct
mistralai/mistral-nemo
mistralai/mistral-small-24b-instruct-2501
openai/gpt-oss-20b:free
openai/gpt-oss-120b:free
```
:::
:::{tab-item} Runpod Serverless
Use Gemma3 on [Runpod Serverless].
```shell
export LLM_PROVIDER="runpod_serverless"
export LLM_ENDPOINT="https://api.runpod.ai/v2/<YOUR_RUNPOD_ENDPOINT_ID>/openai/v1"
export LLM_NAME="gemma3:270m"
export RUNPOD_API_KEY="<YOUR_RUNPOD_API_KEY>"
```
:::
::::

### API

A sketch to use NLSQL from Python programs.

```python
import sqlalchemy as sa
from cratedb_toolkit.query.nlsql.api import DataQuery
from cratedb_toolkit.query.nlsql.model import DatabaseInfo, ModelInfo, ModelProvider

# Configure database.
# For connecting to CrateDB on localhost, use a connection string like this:
engine = sa.create_engine("crate://crate:crate@localhost:4200/?ssl=false")
# For connecting to CrateDB Cloud, use a connection string like this:
# engine = sa.create_engine("crate://admin:dZ...6LqB@example.eks1.eu-west-1.aws.cratedb.net:4200/?ssl=true")
schema = "doc"

# Configure an LLM-based query engine.
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.ACME, name="foo-frontier-7.1"),
)

# Query database.
response = dataquery.ask("What is the average value for sensor 1?")
print(response)
```

Select and configure the LLM of your choice.

::::{tab-set}
:::{tab-item} Amazon
Use Amazon Nova on [Amazon Bedrock].
```python
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.AMAZON_BEDROCK_CONVERSE, name="global.amazon.nova-2-lite-v1:0"),
)
```
:::
:::{tab-item} Anthropic
Use [Anthropic Claude] Sonnet.
```python
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(
        provider=ModelProvider.ANTHROPIC, 
        name="claude-sonnet-4-0",
        api_key="<YOUR_ANTHROPIC_API_KEY>",
    ),
)
```
:::
:::{tab-item} Azure
Use GPT on [Azure OpenAI].
```python
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(
        provider=ModelProvider.AZURE, 
        name="gpt-4.1", 
        instance="my_embedding-model",
        endpoint="https://acme-openai.openai.azure.com/",
        api_key="<YOUR_AZURE_OPENAI_API_KEY>",
    ),
)
```
:::
:::{tab-item} Google
Use [Gemini Flash] from Google.
```python
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.GOOGLE, name="gemini-2.5-flash"),
)
```
:::
:::{tab-item} Hugging Face Serverless
Use Zephyr on the [Hugging Face Serverless Inference API].
```python
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.HUGGINGFACE_SERVERLESS, name="HuggingFaceH4/zephyr-7b-alpha"),
)
```
:::
:::{tab-item} Mistral
Use models from [Mistral AI].
```python
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.MISTRAL, name="mistral-medium-latest"),
)
```
:::
:::{tab-item} Ollama
Use [Ollama] to run models on your own machines, for example Gemma3.
```python
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.OLLAMA, name="gemma3:1b"),
)
```
:::
:::{tab-item} OpenAI
Use [GPT‑4o mini] from [OpenAI].
```python
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.OPENAI, name="gpt-4o-mini"),
)
```
:::
:::{tab-item} OpenRouter
Choose from many models available via [OpenRouter], for example Gemma3.
```shell
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(provider=ModelProvider.OPENROUTER, name="google/gemma-3-4b-it:free"),
)
```
:::
:::{tab-item} Runpod Serverless
Use Gemma3 on [Runpod Serverless].
```python
dataquery = DataQuery(
    db=DatabaseInfo(engine=engine, schema=schema),
    model=ModelInfo(
        provider=ModelProvider.RUNPOD_SERVERLESS,
        name="gemma3:270m",
        endpoint="https://api.runpod.ai/v2/<YOUR_RUNPOD_ENDPOINT_ID>/openai/v1",
        api_key="<YOUR_RUNPOD_API_KEY>",
    ),
)
```
:::
::::

## Examples

The {ref}`nlsql-example-sensor` demonstrates a basic database inquiry 
using the question »What is the average value for sensor 1?« to acquire
information from a single table.

{ref}`nlsql-example-employee`, {ref}`nlsql-example-product`, and
{ref}`nlsql-example-weather` explore and demonstrate other kinds
of query variants.


```{toctree}
:maxdepth: 1
:hidden:

Employee data example <example-employee>
Product orders example <example-product>
Sensor data example <example-sensor>
Weather data example <example-weather>
```


[Amazon Bedrock]: https://docs.aws.amazon.com/bedrock/
[Anthropic Claude]: https://platform.claude.com/docs/
[Azure OpenAI]: https://azure.microsoft.com/en-gb/pricing/details/azure-openai/
[Gemini Flash]: https://deepmind.google/models/gemini/flash/
[GPT‑4o mini]: https://openai.com/index/gpt-4o-mini-advancing-cost-efficient-intelligence/
[Hugging Face Serverless Inference API]: https://huggingface.co/learn/cookbook/enterprise_hub_serverless_inference_api
[Mistral AI]: https://mistral.ai/
[Ollama]: https://ollama.com/
[OpenAI]: https://openai.com/
[OpenRouter]: https://openrouter.ai/
[Prompt-to-SQL Injections]: https://syssec.dpss.inesc-id.pt/papers/pedro_icse25.pdf
[QueryData]: https://cloud.google.com/blog/products/databases/introducing-querydata-for-near-100-percent-accurate-data-agents
[Runpod Serverless]: https://www.runpod.io/product/serverless
[Vanna AI]: https://vanna.ai/
