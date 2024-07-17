---
orphan: true
---

(mongodb-services)=
# Services CrateDB and MongoDB

In order to exercise this subsystem, you will need one running instance of
each CrateDB and MongoDB. You can either deploy them on your workstation,
or by using their managed service infrastructure.


(mongodb-services-standalone)=
## Standalone Services
Quickly start CrateDB and MongoDB using Docker or Podman.

### CrateDB
Start CrateDB.
```shell
docker run --rm -it --name=cratedb --publish=4200:4200 --env=CRATE_HEAP_SIZE=2g \
    crate:5.7 -Cdiscovery.type=single-node
```

### MongoDB
Start MongoDB.
Please note that change streams are only available for replica sets and
sharded clusters, so let's define a replica set by using the
`--replSet rs-testdrive` option when starting the MongoDB server.
```shell
docker run -it --rm --name=mongodb --publish=27017:27017 \
    mongo:7 mongod --replSet rs-testdrive
```

Now, initialize the replica set, by using the `mongosh` command to invoke
the `rs.initiate()` operation.
```shell
export MONGODB_URL="mongodb://localhost/"
docker run -i --rm --network=host mongo:7 mongosh ${MONGODB_URL} <<EOF

config = {
    _id: "rs-testdrive",
    members: [{ _id : 0, host : "localhost:27017"}]
};
rs.initiate(config);

EOF
```


(mongodb-services-cloud)=
## Cloud Services
Quickly provision [CrateDB Cloud] and [MongoDB Atlas].

### CrateDB Cloud
To provision a database cluster, use either the [croud CLI], or the
[CrateDB Cloud Web Console].

Invoke CLI login.
```shell
croud login
```
Create organization.
```shell
croud organizations create --name samplecroudorganization
```
Create project.
```shell
croud projects create --name sampleproject
```
Deploy cluster.
```shell
croud clusters deploy \
  --product-name crfree \
  --tier default \
  --cluster-name testdrive \
  --subscription-id 782dfc00-7b25-4f48-8381-b1b096dd1619 \
  --project-id 952cd102-91c1-4837-962a-12ecb71a6ba8 \
  --version 5.8.0 \
  --username admin \
  --password "as6da9ddasfaad7i902jcv780dmcba"
```

When shutting down your workbench, you may want to clean up any cloud resources
you just used.
```shell
croud clusters delete --cluster-id CLUSTER_ID
```

### MongoDB Atlas
To provision a database cluster, use either the [Atlas CLI], or the
Atlas User Interface.

Create an API key.
```shell
atlas projects apiKeys create --desc "Ondemand Testdrive" --role GROUP_OWNER
```
```text
API Key '889727cb5bfe8830d0f8a203' created.
Public API Key bksttjep
Private API Key 9f8c1c41-b5f7-4d2a-b1a0-a1d2ef457796
```
Enter authentication key information.
```shell
atlas config init
```
Create database cluster.
```shell
atlas clusters create testdrive --provider AWS --region EU_CENTRAL_1 --tier M0 --tag env=dev
```
Inquire connection string.
```shell
atlas clusters connectionStrings describe testdrive
```
```text
mongodb+srv://testdrive.jaxmmfp.mongodb.net
```

Finally, create a "Database Access" user using the Atlas Web Console. Please
take a note of the credentials to populate them into `MONGODB_URL` and
`MONGODB_URL_CTK` environment variables, e.g. for the {ref}`MongoDB CDC Relay
Tutorial <mongodb-cdc-cloud>`.

When shutting down your workbench, you may want to clean up any cloud resources
you just used.
```shell
atlas clusters delete testdrive
```


:::{todo}
- Evaluate and describe how to perform the "Database Access" step using the Atlas CLI
  instead of using the Atlas Web Console.
:::


[Atlas CLI]: https://www.mongodb.com/docs/atlas/cli/ 
[CrateDB Cloud]: https://cratedb.com/docs/cloud/
[croud CLI]: https://cratedb.com/docs/cloud/en/latest/tutorials/deploy/croud.html
[MongoDB Atlas]: https://www.mongodb.com/atlas
[CrateDB Cloud Web Console]: https://cratedb.com/docs/cloud/en/latest/tutorials/quick-start.html#deploy-cluster
