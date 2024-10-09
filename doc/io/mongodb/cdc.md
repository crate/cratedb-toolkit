(mongodb-cdc-relay)=
# MongoDB CDC Relay

## About
Relay a [MongoDB Change Stream] into a [CrateDB] table using a one-stop command
`ctk load table mongodb+cdc://...`, or `mongodb+srv+cdc://` for MongoDB Atlas.

You can use it in order to facilitate convenient data transfers to be used
within data pipelines or ad hoc operations. It can be used as a CLI interface,
and as a library.


## Install
```shell
pip install --upgrade 'cratedb-toolkit[mongodb]'
```

:::{tip}
The tutorial also uses the programs `crash`, `mongosh`, and `atlas`. `crash`
will be installed with CrateDB Toolkit, but `mongosh` and `atlas` must be
installed by other means. If you are using Docker anyway, please use those
command aliases to provide them to your environment without actually needing
to install them.

```shell
alias mongosh='docker run -i --rm --network=host mongo:7 mongosh'
```

The `atlas` program needs to store authentication information between invocations,
therefore you need to supply a storage volume.
```shell
mkdir atlas-config
alias atlas='docker run --rm -it --volume=$(pwd)/atlas-config:/root mongodb/atlas atlas'
```
:::


## Usage

(mongodb-cdc-workstation)=
### Workstation
The guidelines assume that both services, CrateDB and MongoDB, are listening on
`localhost`.
Please find guidelines how to provide them on your workstation using
Docker or Podman in the {ref}`mongodb-services-standalone` section.
```shell
export MONGODB_URL=mongodb://localhost/testdrive
export MONGODB_URL_CTK=mongodb+cdc://localhost/testdrive/demo
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost/testdrive/demo-cdc
ctk load table "${MONGODB_URL_CTK}"
```

Insert document into MongoDB collection, and update it.
```shell
mongosh "${MONGODB_URL}" --eval 'db.demo.insertOne({"foo": "bar"})'
mongosh "${MONGODB_URL}" --eval 'db.demo.updateOne({"foo": "bar"}, { $set: { status: "D" } })'
```

Query data in CrateDB.
```shell
crash --command 'SELECT * FROM "testdrive"."demo-cdc";'
```

Invoke a delete operation, and check data in CrateDB once more.
```shell
mongosh "${MONGODB_URL}" --eval 'db.demo.deleteOne({"foo": "bar"})'
crash --command 'SELECT * FROM "testdrive"."demo-cdc";'
```

(mongodb-cdc-cloud)=
### Cloud
The guidelines assume usage of cloud variants for both services, CrateDB Cloud
and MongoDB Atlas.
Please find guidelines how to provision relevant cloud resources in the
{ref}`mongodb-services-cloud` section. You will need authentication credentials
from this step for the next one.

:::{rubric} Invoke pipeline
:::
A canonical invocation for ingesting MongoDB Atlas Change Streams into
CrateDB Cloud.

```shell
export MONGODB_URL=mongodb+srv://user:password@testdrive.jaxmmfp.mongodb.net/testdrive
export MONGODB_URL_CTK=mongodb+srv+cdc://user:password@testdrive.jaxmmfp.mongodb.net/testdrive/demo
export CRATEDB_HTTP_URL="https://admin:dZ...6LqB@testdrive.eks1.eu-west-1.aws.cratedb.net:4200/"
export CRATEDB_SQLALCHEMY_URL="crate://admin:dZ...6LqB@testdrive.eks1.eu-west-1.aws.cratedb.net:4200/testdrive/demo-cdc?ssl=true"
```
```shell
ctk load table "${MONGODB_URL_CTK}"
```

:::{note}
Please note the `mongodb+srv://` and `mongodb+srv+cdc://` URL schemes, and the
`ssl=true` query parameter. Both are needed to establish connectivity with
MongoDB Atlas and CrateDB.
:::

:::{rubric} Trigger CDC events
:::
Inserting a document into the MongoDB collection, and updating it, will trigger two CDC events.
```shell
mongosh "${MONGODB_URL}" --eval 'db.demo.insertOne({"foo": "bar"})'
mongosh "${MONGODB_URL}" --eval 'db.demo.updateOne({"foo": "bar"}, { $set: { status: "D" } })'
```

:::{rubric} Query data in CrateDB
:::
```shell
crash --hosts "${CRATEDB_HTTP_URL}" --command 'SELECT * FROM "testdrive"."demo-cdc";'
```


## Transformations
You can use [Zyp Transformations] to change the shape of the data while being
transferred. In order to add it to the pipeline, use the `--transformation`
command line option.


## Appendix
A few operations that are handy when exploring this exercise.

### Database Services
Provide CrateDB and MongoDB services.
- See {ref}`mongodb-services`.

### Database Operations

Query records in CrateDB table.
```shell
crash --command 'SELECT * FROM "testdrive"."demo-cdc";'
```

Truncate CrateDB table.
```shell
crash --command 'DELETE FROM "testdrive"."demo-cdc";'
```

Query documents in MongoDB collection.
```shell
mongosh "${MONGODB_URL}" --eval 'db.demo.find()'
```

Truncate MongoDB collection.
```shell
mongosh "${MONGODB_URL}" --eval 'db.demo.drop()'
```


## Backlog
:::{todo}
- Improve general CLI UX/DX, for example by using `ctk shell`.
- Provide [SDK and CLI for CrateDB Cloud Cluster APIs], for improving Cloud DX.
:::


[commons-codec]: https://pypi.org/project/commons-codec/
[CrateDB]: https://cratedb.com/docs/guide/home/
[CrateDB Cloud]: https://cratedb.com/docs/cloud/
[MongoDB Atlas]: https://www.mongodb.com/atlas
[MongoDB Change Stream]: https://www.mongodb.com/docs/manual/changeStreams/
[SDK and CLI for CrateDB Cloud Cluster APIs]: https://github.com/crate-workbench/cratedb-toolkit/pull/81
[Zyp Transformations]: https://commons-codec.readthedocs.io/zyp/
