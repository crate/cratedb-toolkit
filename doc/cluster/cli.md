(cluster-api-cli)=

# CrateDB Cluster CLI

The `ctk cluster {start,info,stop}` subcommands provide higher level CLI
entrypoints to start/deploy/resume a database cluster, inquire information
about it, and stop/suspend it again.

The subsystem is implemented on top of the {ref}`croud:index` application,
which gets installed along the lines and is used later on this page.

## Install

We recommend using the [uv] package manager to install the application per
`uv tool install`. Otherwise, using `pipx install` or `pip install --user`
are viable alternatives.
```shell
uv tool install --upgrade 'cratedb-toolkit'
```

## Authenticate

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

## Configure

The `ctk cluster` subcommand accepts configuration settings per CLI options and
environment variables.

:::{include} ../cluster/_address.md
:::

## Usage

Start or resume a cluster, deploying it on demand if it doesn't exist.
```shell
ctk cluster start --cluster-name hotzenplotz
```

Display cluster information.
```shell
ctk cluster info --cluster-name hotzenplotz
```

Stop (suspend) a cluster.
```shell
ctk cluster stop --cluster-name hotzenplotz
```

:::{seealso}
{ref}`cluster-api-tutorial` includes a full end-to-end tutorial.
:::


[CrateDB Cloud]: https://cratedb.com/docs/cloud/
[CrateDB Cloud Console]: https://console.cratedb.cloud/
[uv]: https://docs.astral.sh/uv/
