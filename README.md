# Data retention for CrateDB


## About

A data retention subsystem for CrateDB, providing different strategies.

In the [dags](dags) directory you can find a subset of our DAG examples.
Each DAG is accompanied by a tutorial:

* [data_retention_delete_dag.py](cratedb_retentions/strategy/delete.py) ([tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-implementation-of-data-retention-policy/913)): implements a retention policy algorithm that drops expired partitions
* [data_retention_reallocate_dag.py](dags/data_retention_reallocate_dag.py) ([tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-building-a-hot-cold-storage-data-retention-policy/934)): implements a retention policy algorithm that reallocates expired partitions from hot nodes to cold nodes
* [data_retention_snapshot_dag.py](dags/data_retention_snapshot_dag.py): implements a retention policy algorithm that snapshots expired partitions to a repository


## Setup

Acquire sources.
```shell
git clone https://github.com/crate-workbench/cratedb-retentions
cd cratedb-retentions
```

Install project.
```shell
pip install --editable=.[develop,test]
```

Install retention policy bookkeeping tables.
```shell
cratedb-retentions setup "crate://localhost/"
```


## Usage
```shell
docker run --rm -i --network=host crate crash <<SQL
    INSERT INTO retention_policies (
      table_schema, table_name, partition_column, retention_period, strategy)
    VALUES ('doc', 'raw_metrics', 'ts_day', 1, 'delete');
SQL
```

```shell
cratedb-retentions run --day=2023-06-27 --strategy=delete "crate://localhost"
```

## Development

Run tests.
```shell
export TC_KEEPALIVE=true
poe check
```

Format code.
```shell
poe format
```
