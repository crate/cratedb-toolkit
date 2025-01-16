(install)=
# Installation

It is recommended to use [uv] to install Python packages,
which is an extremely fast Python package and project manager.

Install package with only [fundamental dependencies].
```shell
uv pip install --upgrade 'cratedb-toolkit'
```

Install package including [full dependencies] and [all subsystems].
```shell
uv pip install --upgrade 'cratedb-toolkit[all]'
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

Alternatively, use the Docker image per `ghcr.io/crate/cratedb-toolkit`.

Run with Docker or Podman.
```shell
docker run --rm -it --pull=always "ghcr.io/crate/cratedb-toolkit" ctk --version
```

To conveniently use CrateDB Toolkit on your workstation without installing it.
```shell
alias ctk='docker run --rm -it --network=host "ghcr.io/crate/cratedb-toolkit" ctk'
```

Run on Kubernetes.
:::{todo}
Add a quick description how to whip CTK into a K8s service unit.
:::


[fundamental dependencies]: https://github.com/crate/cratedb-toolkit/blob/v0.0.30/pyproject.toml#L85-L110
[full dependencies]: https://github.com/crate/cratedb-toolkit/blob/v0.0.30/pyproject.toml#L148-L150
[all subsystems]: https://github.com/crate/cratedb-toolkit/blob/v0.0.30/pyproject.toml#L112-L114
[uv]: https://docs.astral.sh/uv/
