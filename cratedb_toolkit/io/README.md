# Load and extract data into/from CrateDB


## Synopsis

Define the cluster id of your CrateDB Cloud Cluster, and connection credentials
```shell
export CRATEDB_CLOUD_CLUSTER_ID=e1e38d92-a650-48f1-8a70-8133f2d5c400
export CRATEDB_USERNAME=admin
export CRATEDB_PASSWORD=3$MJ5fALP8bNOYCYBMLOrzd&
```

Load data.
```shell
ctk load table https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_weather.csv.gz
```
