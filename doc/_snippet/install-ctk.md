## Install

CrateDB Toolkit uses the Python programming language, which is installed
on most machines today. Otherwise, we recommend to [download and install
Python][install-python] from the original source.
For installing additional Python packages, we recommend to
[install the uv package manager][install-uv].
```shell
uv tool install --upgrade 'cratedb-toolkit'
```

An alternative way to install Python packages is to use [pipx]
or `pip install --user`.
```shell
pipx install 'cratedb-toolkit'
```


[install-python]: https://www.python.org/downloads/
[install-uv]: https://docs.astral.sh/uv/getting-started/installation/
[pipx]: https://pipx.pypa.io/
