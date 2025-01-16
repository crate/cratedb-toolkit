(cluster-info)=
# CrateDB Cluster Information

A bundle of information inquiry utilities, for diagnostics and more.

## Install
```shell
pip install --upgrade 'cratedb-toolkit'
```
:::{tip}
Alternatively, use the Docker image per `ghcr.io/crate/cratedb-toolkit`.
For more information about installing CrateDB Toolkit, see {ref}`install`.
:::

## Synopsis

Define CrateDB database cluster address per command-line option. Choose one of both alternatives.
```shell
ctk cfr --cratedb-http-url "https://username:password@localhost:4200/?schema=ext" jobstats collect
ctk cfr --cratedb-sqlalchemy-url "crate://username:password@localhost:4200/?schema=ext&ssl=true" jobstats collect
```

Define CrateDB database cluster address per aa. Choose one of both alternatives.
```shell
export CRATEDB_HTTP_URL=https://username:password@localhost:4200/?schema=ext
```
```shell
export CRATEDB_SQLALCHEMY_URL=crate://username:password@localhost:4200/?schema=ext&ssl=true
```
:::{note}
For some commands, both options might not be available yet, just one of them.
:::


### One shot commands
Display system and database cluster information.
```shell
ctk info cluster
```

Display database cluster job information.
```shell
ctk info jobs
```

Display database cluster log messages.
```shell
ctk info logs
```

Display the most recent entries of the `sys.jobs_log` table,
optionally polling it for updates by adding `--follow`.
For more information, see [](#tail).
```shell
ctk tail -n 3 sys.jobs_log
```


## HTTP API

Install.
```shell
pip install --upgrade 'cratedb-toolkit[service]'
```

Expose collected status information.
```shell
ctk info serve
```
Consume cluster information via HTTP.
```shell
http http://127.0.0.1:4242/info/all
```

Make the service listen on a specific address.
```shell
ctk info serve --listen 0.0.0.0:8042
```

:::{note}
The `--reload` option is suitable for development scenarios where you intend
to have the changes to the code become available while editing, in near
real-time.
```shell
ctk info --debug serve --reload
```
:::
