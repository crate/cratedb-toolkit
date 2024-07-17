# Development Sandbox

## Introduction

Acquire sources.
```shell
git clone https://github.com/crate/cratedb-toolkit
cd cratedb-toolkit
```

It is recommended to use a Python virtualenv for the subsequent operations.
If you something gets messed up during development, it is easy to nuke the
installation, and start from scratch.
```shell
python3 -m venv .venv
source .venv/bin/activate
```

Install project in sandbox mode.
```shell
pip install --editable='.[all,develop,docs,test]'
```

Run tests. `TC_KEEPALIVE` keeps the auxiliary service containers running, which
speeds up runtime on subsequent invocations. Note that the test suite uses the
`testdrive-ext` schema for storing the retention policy table, and the
`testdrive-data` schema for storing data tables.
```shell
export TC_KEEPALIVE=true
poe check
```

In order to shut down and destroy the auxiliary service containers, which have
been started by running the test suite, and were kept running by using 
`TC_KEEPALIVE`, use this command.
```shell
docker rm --force testcontainers-cratedb testcontainers-mongodb
```

Format code.
```shell
poe format
```


## Troubleshooting
Docker is needed to provide service instances to the test suite. If your Docker
daemon is not running or available, you will receive an error message like that:
```python
docker.errors.DockerException: Error while fetching server API version:
    ('Connection aborted.', FileNotFoundError(2, 'No such file or directory'))
AttributeError: 'CrateDBContainer' object has no attribute '_container'
```
In order to fix the problem, just start your Docker daemon.
