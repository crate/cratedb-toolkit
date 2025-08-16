(install)=
# Installation

It is recommended to use [uv] to install Python packages,
which is an extremely fast Python package and project manager.

## GA packages

Install package with only [fundamental dependencies].
```shell
uv tool install --upgrade 'cratedb-toolkit'
```

Install package including [all subsystems] / [full dependencies].
```shell
uv tool install --upgrade 'cratedb-toolkit[all]'
```

Verify installation.
```shell
cratedb-toolkit --version
```

The package also installs a shortcut alias `ctk`.
```shell
ctk --version
```

## Container

Alternatively, use a container image from GHCR. Use `ghcr.io/crate/cratedb-toolkit`
for the default toolset. To use the "ingest" image (which includes the `io-ingestr`
extras and the ingestr-based I/O adapters), use `ghcr.io/crate/cratedb-toolkit-ingest`.
The default image does not include these extras or drivers.

Run with Docker or Podman.
```shell
docker run --rm -it --pull=always "ghcr.io/crate/cratedb-toolkit" ctk --version
```
```shell
docker run --rm -it --pull=always "ghcr.io/crate/cratedb-toolkit-ingest" ctk --version
```

To conveniently use CrateDB Toolkit on your workstation without installing it.
```shell
alias ctk='docker run --rm -it --network=host "ghcr.io/crate/cratedb-toolkit" ctk'
alias ctk-ingest='docker run --rm -it --network=host "ghcr.io/crate/cratedb-toolkit-ingest" ctk'
```

Run on Kubernetes.
:::{todo}
Add a quick description how to whip CTK into a K8s service unit.
:::

## Git

You can easily install the latest development version from the Git repository.
This example command selects the `cfr` extra for demonstration purposes.
```shell
uv tool install 'cratedb-toolkit[cfr] @ git+https://github.com/crate/cratedb-toolkit'
```

## Dependency / subsystem selection

To install subsets of dependencies selectively, please choose amongst the possible
Python package "extras" that relate to corresponding subsystems:
`cfr`, `cloud`, `datasets`, `docs-api`, `dynamodb`, `influxdb`, `io`,
`kinesis`, `mcp`, `mongodb`, `pymongo`, `service`, `testing`.


[fundamental dependencies]: https://github.com/crate/cratedb-toolkit/blob/v0.0.30/pyproject.toml#L85-L110
[full dependencies]: https://github.com/crate/cratedb-toolkit/blob/v0.0.30/pyproject.toml#L148-L150
[all subsystems]: https://github.com/crate/cratedb-toolkit/blob/v0.0.30/pyproject.toml#L112-L114
[uv]: https://docs.astral.sh/uv/
