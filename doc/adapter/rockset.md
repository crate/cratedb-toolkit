# Rockset Adapter for CrateDB


## About

Because the [Rockset HTTP API is sunsetting on September 30th, 2024](
https://docs.rockset.com/documentation/docs/faq), and [CrateDB] covers a
reasonable amount of features, this is an experiment to wrap its features
through a corresponding emulation layer, so client programs and libraries
may work unmodified.


## What's Inside

An incubating API mock, emulating the most basic primitives of the [Rockset
HTTP API], using [CrateDB] as a database backend.

While the [API coverage](#rockset-api-coverage) is thin yet, the performance is
paramount. There are no [API Rate Limits] or [Ingest Related Limits] in place.

:::{warning}
**Please note this is pre-alpha software, and is only intended for demonstration
purposes.**
<br>
**Here be dragons. You have been warned.**
:::

## Synopsis

Point your Rockset client to `http://localhost:4243`, like demonstrated in the
Python example snippet below.
```python
import os
from enum import Enum

from rockset import Configuration, RocksetClient


# Define endpoint of API Emulator.
class Connections(str, Enum):
    rockset_apiserver = os.environ.get("ROCKSET_APISERVER", "http://localhost:4243")
    rockset_apikey = os.environ.get("ROCKSET_APIKEY", "abc123")


def main():

    # Connect to the Rockset HTTP API.
    rs_config = Configuration(
        host=Connections.rockset_apiserver,
        api_key=Connections.rockset_apikey,
    )
    rs = RocksetClient(config=rs_config)

    # Run a few operations.
    rs.Documents.add_documents(...)
    rs.Queries.query(...)
```

:::{note}
Currently, the API key has no significance.
:::


## Usage

Install the emulation package.
```shell
pip install --upgrade 'cratedb-toolkit[service]'
```

Start CrateDB.
```shell
docker run --rm -it --name=cratedb --publish=4200:4200 --env=CRATE_HEAP_SIZE=2g \
  crate/crate:nightly -Cdiscovery.type=single-node
```

Start the API server, by default listening on `localhost:4243`. Optionally,
use the `--listen` option to configure host and port number.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://localhost/
ctk rockset serve
```

Invoke one of the client example programs enumerated below,
for example the [Python example program].


(rockset-api-coverage)=
## API Coverage

The most basic primitives: Loading data and querying it back.

- [Add Documents]
- [Execute SQL Query]


## Full Examples

Use the full example programs, subsequently exercising
`DocumentsApi.add_documents` and `QueriesApi.query` operations.

- [CLI example program]
- [Java example program]
- [JavaScript example program]
- [Python example program]
- [Shell example program]

The HTTP API emulator works equally well with Rockset client programs
written in any programming language, using either Rockset SDK packages,
or plain HTTP conversations.

- https://docs.rockset.com/documentation/docs/libraries-tools


[Add Documents]: https://docs.rockset.com/documentation/reference/adddocuments
[API Rate Limits]: https://docs.rockset.com/documentation/docs/api-rate-limits
[CLI example program]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/examples/rockset/cli/basic.sh
[CrateDB]: https://cratedb.com/database
[Execute SQL Query]: https://docs.rockset.com/documentation/reference/query
[Ingest Related Limits]: https://docs.rockset.com/documentation/docs/ingest-related-limits
[Java example program]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/examples/rockset/java/Basic.java
[JavaScript example program]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/examples/rockset/javascript/basic.js
[Python example program]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/examples/rockset/python/basic.py
[Rockset HTTP API]: https://docs.rockset.com/documentation/reference/rest-api
[Shell example program]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/examples/rockset/shell/basic.sh
