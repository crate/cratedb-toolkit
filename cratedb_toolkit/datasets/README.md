# CrateDB Datasets API

Provide access to datasets, to be easily consumed by tutorials
and/or production applications.

## Synopsis
```python
from cratedb_toolkit.datasets import load_dataset

dataset = load_dataset("tutorial/weather-basic")
dataset.to_cratedb(dburi="crate://crate@localhost/", table="weather_data")
```
