(mongodb-loader)=
# MongoDB Table Loader

## About
Load data from MongoDB into CrateDB using a one-stop command
`ctk load table mongodb://...`, in order to facilitate convenient
data transfers to be used within data pipelines or ad hoc operations.

## Install
```shell
pip install --upgrade 'cratedb-toolkit[mongodb]'
```

## Example
Import two data points into MongoDB.

```shell
mongosh mongodb://localhost:27017/testdrive <<EOF
db.demo.remove({})
db.demo.insertMany([
  {
    timestamp: new Date(1556896326),
    region: "amazonas",
    temperature: 42.42,
    humidity: 84.84,
  },
  {
    timestamp: new Date(1556896327),
    region: "amazonas",
    temperature: 45.89,
    humidity: 77.23,
    windspeed: 5.4,
  },
])
db.demo.find({})
EOF
```

Transfer data from MongoDB database/collection into CrateDB schema/table.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk load table mongodb://localhost:27017/testdrive/demo
```

Query data in CrateDB.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk shell --command "SELECT * FROM testdrive.demo;"
ctk show table "testdrive.demo"
```


:::{todo}
Use `mongoimport`.
```shell
mongoimport --uri 'mongodb+srv://MYUSERNAME:SECRETPASSWORD@mycluster-ABCDE.azure.mongodb.net/test?retryWrites=true&w=majority'
```
:::
