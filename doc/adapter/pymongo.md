# PyMongo Adapter for CrateDB


## About

[Mongo DB Is Web Scale], CrateDB too. Because CrateDB covers a reasonable
amount of features of MongoDB, this is an experiment to provide an emulation
layer for the PyMongo driver, so Python client programs and libraries may
work unmodified.


## What's Inside

An amalgamated [PyMongo] driver, using [CrateDB] as a backend instead of [MongoDB].
It can run 95% of the [MongoDB "Getting Started" tutorial] successfully.

While coverage of the complete MongoDB API, and more advanced query translations,
are pretty thin yet, freedom and performance are paramount. There are no [Atlas
Service Limits] or [MongoDB Limits and Thresholds] in place.

:::{warning}
**Please note this is pre-alpha software, and is only intended for demonstration
purposes.**
<br>
**Here be dragons. You have been warned.**
:::


## Synopsis

Invoke your PyMongo-based application like this, using `PyMongoCrateDBAdapter`
to wrap the invocation of `pymongo.MongoClient(...)`, pointing the client to
a CrateDB database server instead.
```python
import pymongo
from cratedb_toolkit.adapter.pymongo import PyMongoCrateDBAdapter

with PyMongoCrateDBAdapter(dburi="crate://crate@localhost:4200"):
    client = pymongo.MongoClient("localhost", 27017)
    collection = client.foo.bar

    inserted_id = collection.insert_one({"author": "Mike", "text": "My first blog post!"}).inserted_id
    print(inserted_id)

    document = collection.find_one({"author": "Mike"})
    print(document)
```


## Usage

Install the emulation package.
```shell
pip install --upgrade 'cratedb-toolkit[pymongo]'
```

Start CrateDB.
```shell
docker run --rm -it --name=cratedb \
  --publish=4200:4200 --publish=5432:5432 \
  --env=CRATE_HEAP_SIZE=2g crate/crate:nightly \
  -Cdiscovery.type=single-node
```

Invoke a Python-based program connecting to MongoDB using the `pymongo`
package, for example the [Python example program], which is exercising a few
basic operations.


(pymongo-api-coverage)=
## API Coverage

The most basic primitives: Loading data and querying it back.

- [MongoDB "Getting Started" tutorial]


## Examples

To inspect and explore the capabilities of the driver adapter, please use the
basic [Python example program], and relevant [test cases] of the adapter
subsystem.


[Atlas Service Limits]: https://www.mongodb.com/docs/atlas/reference/atlas-limits/
[CrateDB]: https://cratedb.com/database
[MongoDB]: https://www.mongodb.com/products/self-managed/community-edition
[Mongo DB Is Web Scale]: https://www.youtube.com/watch?v=b2F-DItXtZs
[MongoDB Limits and Thresholds]: https://www.mongodb.com/docs/manual/reference/limits/
[MongoDB "Getting Started" tutorial]: https://pymongo.readthedocs.io/en/stable/tutorial.html
[PyMongo]: https://github.com/mongodb/mongo-python-driver
[Python example program]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/examples/pymongo_adapter.py
[test cases]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/tests/adapter/test_pymongo.py
