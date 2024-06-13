(wtf)=
# CrateDB WTF

A diagnostics utility in the spirit of [git-wtf], [grafana-wtf], and [pip.wtf].
It is still a work-in-progress, but it is usable already.

## Install
```shell
pip install --upgrade 'cratedb-toolkit'
```
Alternatively, use the Docker image at `ghcr.io/crate-workbench/cratedb-toolkit`.

## Synopsis

Define CrateDB database cluster address.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://localhost/
```

Display system and database cluster information.
```shell
cratedb-wtf info
```

Display database cluster job information.
```shell
cratedb-wtf job-info
```

Display database cluster log messages.
```shell
cratedb-wtf logs
```

Collect and display job statistics.
```shell
cratedb-wtf job-statistics collect
cratedb-wtf job-statistics view
```

Record complete outcomes of `info` and `job-info`.
```shell
cratedb-wtf record
```


## HTTP API

Install.
```shell
pip install --upgrade 'cratedb-toolkit[service]'
```

Expose collected status information.
```shell
cratedb-wtf --debug serve --reload
```
Consume collected status information via HTTP.
```shell
http http://127.0.0.1:4242/info/all
```

Make the service listen on a specific address.
```shell
ctk wtf serve --listen 0.0.0.0:8042
```

:::{note}
The `--reload` option is suitable for development scenarios where you intend
to have the changes to the code become available while editing, in near
real-time.
:::


```{toctree}
:maxdepth: 1
:hidden:

backlog
```



[git-wtf]: http://thrawn01.org/posts/2014/03/03/git-wtf/ 
[grafana-wtf]: https://github.com/panodata/grafana-wtf
[pip.wtf]: https://github.com/sabslikesobs/pip.wtf
