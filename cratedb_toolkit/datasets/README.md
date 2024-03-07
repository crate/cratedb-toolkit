# CrateDB Datasets API

Provide access to datasets, to be easily consumed by tutorials
and/or production applications.

## Synopsis

```python
from cratedb_toolkit.datasets import load_dataset

dataset = load_dataset("tutorial/weather-basic")
dataset.dbtable(dburi="crate://crate@localhost/", table="weather_data").load()
```

```python
from cratedb_toolkit.datasets import load_dataset

dataset = load_dataset("kaggle://guillemservera/global-daily-climate-data/daily_weather.parquet")
dataset.dbtable(dburi="crate://crate@localhost/", table="kaggle_daily_weather").create()
```
