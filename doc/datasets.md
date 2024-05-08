# Datasets API

Provide access to datasets, to be easily consumed by tutorials
and/or production applications.

## Install
```shell
pip install --upgrade 'cratedb-toolkit[datasets]'
```

## Synopsis

```python
from cratedb_toolkit.datasets import load_dataset

dataset = load_dataset("tutorial/weather-basic")
print(dataset.ddl)
```

## Usage

### Built-in datasets
Load an example dataset into a CrateDB database table.
```python
from cratedb_toolkit.datasets import load_dataset

dataset = load_dataset("tutorial/weather-basic")
dataset.dbtable(dburi="crate://crate@localhost/", table="weather_data").load()
```

### Kaggle
For accessing datasets on Kaggle, you will need an account on their platform.

#### Authentication
Either create a configuration file `~/.kaggle/kaggle.json` in JSON format,
```json
{"username":"acme","key":"134af98bdb0bd0fa92078d9c37ac8f78"}
```
or, alternatively, use those environment variables.
```shell
export KAGGLE_USERNAME=acme
export KAGGLE_KEY=134af98bdb0bd0fa92078d9c37ac8f78
```

#### Acquisition
Load a dataset on Kaggle into a CrateDB database table.
```python
from cratedb_toolkit.datasets import load_dataset

dataset = load_dataset("kaggle://guillemservera/global-daily-climate-data/daily_weather.parquet")
dataset.dbtable(dburi="crate://crate@localhost/", table="kaggle_daily_weather").load()
```


## In Practice

Please refer to those notebooks to learn how `load_dataset` works in practice.

- [How to Build Time Series Applications in CrateDB]
- [Exploratory data analysis with CrateDB]
- [Time series decomposition with CrateDB]


[Exploratory data analysis with CrateDB]: https://github.com/crate/cratedb-examples/blob/main/topic/timeseries/exploratory_data_analysis.ipynb
[How to Build Time Series Applications in CrateDB]: https://github.com/crate/cratedb-examples/blob/main/topic/timeseries/dask-weather-data-import.ipynb
[Time series decomposition with CrateDB]: https://github.com/crate/cratedb-examples/blob/main/topic/timeseries/time-series-decomposition.ipynb
