(shell)=

# ctk shell

The database shell interface of CrateDB Toolkit is based on the
{ref}`crash <crash:index>` application. It can connect to both
managed clusters on CrateDB Cloud, and self-hosted instances of
CrateDB.

## Install

We recommend using the [uv] package manager to install the application per
`uv tool install`. Otherwise, using `pipx install` or `pip install --user`
are viable alternatives.
```shell
uv tool install --upgrade 'cratedb-toolkit'
```

## Synopsis

```shell
ctk shell
```

## Usage

The `ctk shell` subcommand accepts configuration settings per CLI options and
environment variables.

:::{include} ../cluster/_address.md
:::

### CrateDB Cloud

When working with [CrateDB Cloud], you can select between two authentication variants.
Either _interactively authorize_ your terminal session using `croud login`,
```shell
croud login --idp {cognito,azuread,github,google}
```
or provide API access credentials per environment variables for _headless/unattended
operations_ after creating them using the [CrateDB Cloud Console] or
`croud api-keys create`.
```shell
# CrateDB Cloud API credentials.
export CRATEDB_CLOUD_API_KEY='<YOUR_API_KEY_HERE>'
export CRATEDB_CLOUD_API_SECRET='<YOUR_API_SECRET_HERE>'
```

Connect to CrateDB Cloud.
```shell
ctk shell --cluster-name hotzenplotz --command "SELECT * from sys.summits LIMIT 2;"
```
```shell
echo "SELECT * from sys.summits LIMIT 2;" | ctk shell --cluster-name testcluster
```

### CrateDB standalone

Connect to a standalone CrateDB instance on localhost, authenticating with the
default user `crate`.
```shell
ctk shell --cluster-url 'crate://localhost:4200' --command "SELECT 42;"
```

When working with self-hosted or standalone [CrateDB] instances, include
authentication credentials into the SQLAlchemy or HTTP connection URLs.
We recommend using the SQLAlchemy connection URL variant.
```shell
export CRATEDB_CLUSTER_URL='https://admin:dZ...6LqB@testdrive.eks1.eu-west-1.aws.cratedb.net:4200/'
```
```shell
export CRATEDB_CLUSTER_URL='crate://admin:dZ...6LqB@testdrive.eks1.eu-west-1.aws.cratedb.net:4200/?ssl=true'
```
When using environment variables to configure ctk, the command itself becomes even shorter.
```shell
ctk shell --command "SELECT 42;"
```


:::{seealso}
{ref}`cluster-api-tutorial` demonstrates a full end-to-end tutorial, which also includes
`ctk shell`.
:::


[CrateDB]: https://cratedb.com/database
[CrateDB Cloud]: https://cratedb.com/docs/cloud/
[CrateDB Cloud Console]: https://console.cratedb.cloud/
[uv]: https://docs.astral.sh/uv/
