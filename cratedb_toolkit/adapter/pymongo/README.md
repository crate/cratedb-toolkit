# Amalgamated PyMongo driver using CrateDB as backend


## Setup

```shell
pip install 'cratedb-toolkit[pymongo]'
```

```shell
docker run --rm -it --name=cratedb \
  --publish=4200:4200 --publish=5432:5432 \
  --env=CRATE_HEAP_SIZE=4g crate/crate:nightly \
  -Cdiscovery.type=single-node
```


## Synopsis

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


## Examples

To inspect the capabilities of the driver adapter, there is a basic program at
[pymongo_adapter.py], and test cases at [test_pymongo.py].


[pymongo_adapter.py]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/examples/pymongo_adapter.py
[test_pymongo.py]: https://github.com/crate-workbench/cratedb-toolkit/blob/main/tests/adapter/test_pymongo.py
