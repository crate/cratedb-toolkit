(hana)=
(sap-hana)=

# SAP HANA

:::{div} sd-text-muted
Load data from SAP HANA into CrateDB.
:::

[SAP HANA] is a high-performance, in-memory data warehousing solution,
including SAP BW/4HANA and native SQL options. It enables real-time
data consolidation, analytics, and reporting.

## Prerequisites

Use Docker or Podman to run all components. This approach works consistently
across Linux, macOS, and Windows.
The tutorial uses [SAP HANA express] to spin up a local instance of HANA for
evaluation purposes.

## Install
Install the most recent Python package [cratedb-toolkit],
or evaluate {ref}`alternative installation methods <install>`.
```shell
uv tool install --upgrade 'cratedb-toolkit[io-ingest]'
```

## Tutorial

5-minute step-by-step instructions about how
to work with SAP HANA and CrateDB.

### Services

Run SAP HANA express and CrateDB using Docker or Podman.
```shell
docker run --rm --name=hana \
  -p 39013:39013 -p 39017:39017 -p 39041-39045:39041-39045 \
  -p 1128-1129:1128-1129 -p 59013-59014:59013-59014 \
  docker.io/saplabs/hanaexpress:latest \
  --master-password HXEHana1 \
  --agree-to-sap-license
```
```shell
docker run --rm --name=cratedb \
  --publish=4200:4200 --publish=5432:5432 --env=CRATE_HEAP_SIZE=2g \
  docker.io/crate:latest '-Cdiscovery.type=single-node'
```

:::{note}
Starting HANA takes a while: It will start responding to requests on port 39017
(system database) when the log output says `HANA is up`, and to port 39041
(tenant database) when it says `Startup finished!`.
:::

### Pre-flight checks
Run basic connectivity check with system database.
```shell
docker exec -it hana bash -ic "hdbsql -i 90 -n localhost:39017 -u SYSTEM -p HXEHana1 'SELECT * FROM sys.dummy'"
```

### Populate data

The SAP HANA database includes a few system tables.
We will select the built-in table `sys.adapters`, so the tutorial can save an
extra step about how to import data into HANA.

### Load data

Let's connect to the system database at `localhost:39017/SYSTEMDB` because it
is available earlier than the tenant database. Otherwise, address the tenant
database using `localhost:39041/HXE`.
```shell
ctk load table \
    "hana://SYSTEM:HXEHana1@localhost:39017/SYSTEMDB?table=sys.adapters" \
    --cluster-url="crate://crate:crate@localhost:4200/testdrive/hana_sys_adapters"
```

### Query data

```shell
crash -c "SHOW CREATE TABLE hana.sys_adapters"
```
```shell
crash -c "SELECT * FROM hana.sys_adapters LIMIT 2"
```

## Documentation

The SAP HANA table name can be provided by using the `&table=` query parameter.
See also the documentation about the [SQLAlchemy Dialect for SAP HANA].

:::{include} ../../_cratedb-options.md
:::

## See also

:::{include} /_snippet/ingest-see-also.md
:::


[cratedb-toolkit]: https://pypi.org/project/cratedb-toolkit/
[SAP HANA]: https://www.sap.com/products/data-cloud/hana/what-is-sap-hana.html
[SAP HANA express]: https://www.sap.com/products/data-cloud/hana/express-trial.html
[SQLAlchemy Dialect for SAP HANA]: https://help.sap.com/docs/SAP_HANA_CLIENT/f1b440ded6144a54ada97ff95dac7adf/01e93e584e524747b570cd9083b08d2b.html
