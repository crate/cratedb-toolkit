# crate-airflow-tutorial
Orchestration Project - Astronomer/Airflow tutorials


## About

This repository contains a few examples of Apache Airflow DAGs for automating recurrent
queries. All DAGs run on Astronomer infrastructure installed on Ubuntu 20.04.3 LTS.

In the [dags](dags) directory you can find a subset of our DAG examples.
Each DAG is accompanied by a tutorial:

* [data_retention_delete_dag.py](dags/data_retention_delete_dag.py) ([tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-implementation-of-data-retention-policy/913)): implements a retention policy algorithm that drops expired partitions
* [data_retention_reallocate_dag.py](dags/data_retention_reallocate_dag.py) ([tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-building-a-hot-cold-storage-data-retention-policy/934)): implements a retention policy algorithm that reallocates expired partitions from hot nodes to cold nodes
* [data_retention_snapshot_dag.py](dags/data_retention_snapshot_dag.py): implements a retention policy algorithm that snapshots expired partitions to a repository


## Development

Install project.
```shell
pip install --editable=.[develop,test]
```

Run tests.
```shell
export TC_KEEPALIVE=true
poe check
```

Format code.
```shell
poe format
```
