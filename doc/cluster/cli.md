(cluster-api-cli)=

# CrateDB Cluster CLI

:::{include} /_snippet/links.md
:::

The `ctk cluster {start,info,stop}` subcommands provide higher level CLI
entrypoints to start/deploy/resume a database cluster, inquire information
about it, and stop/suspend it again.

The subsystem is implemented on top of the {ref}`croud:index` application,
which gets installed along the lines and is used later on this page.

:::{include} /_snippet/install-ctk.md
:::

:::{include} /_snippet/cloud-prerequisites.md
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
