(mongodb-loader)=
# MongoDB Table Loader

## About
Load data from MongoDB and its file formats into CrateDB using a one-stop
command `ctk load table`, in order to facilitate convenient data transfers
to be used within data pipelines or ad hoc operations.

## Coverage
CrateDB Toolkit supports different variants to load MongoDB data from
server instances and filesystems.

- `mongodb://`

  Connect to MongoDB Community or Enterprise Edition.

- `mongodb+srv://`

  Connect to MongoDB Atlas.

- `file+bson://`

  Read files in [MongoDB Extended JSON] or [BSON] format from filesystem.

- `http+bson://`

  Read files in [MongoDB Extended JSON] or [BSON] format from HTTP resource.

## Install
```shell
pip install --upgrade 'cratedb-toolkit[mongodb]'
```

## Usage
The MongoDB I/O adapter can process MongoDB data from different sources.
This section enumerates relevant connectivity options on behalf of
concrete usage examples.

### MongoDB Atlas
Transfer a single collection from MongoDB Atlas.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/ticker/stocks
ctk load table "mongodb+srv://john:EeY6OocooL8rungu@testdrive.ahnaik1.mongodb.net/ticker/stocks?batch-size=5000"
```

Transfer all collections in database from MongoDB Atlas.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/ticker
ctk load table "mongodb+srv://john:EeY6OocooL8rungu@testdrive.ahnaik1.mongodb.net/ticker?batch-size=5000"
```
:::{important}
When transferring **multiple collections**, make sure to use a CrateDB database
address which DOES NOT reference an individual table.
It MUST stop at the **schema** label, here, `ticker`. Likewise, the MongoDB
database address also MUST reference a **database**, NOT a specific collection.
:::

### MongoDB Community and Enterprise
Transfer data from MongoDB database/collection into CrateDB schema/table.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk load table "mongodb://localhost:27017/testdrive/demo"
```
Query data in CrateDB.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk shell --command "SELECT * FROM testdrive.demo;"
ctk show table "testdrive.demo"
```

### MongoDB JSON/BSON files
Load data from MongoDB JSON/BSON files, for example produced by the
`mongoexport` or `mongodump` programs.

```shell
# Extended JSON, filesystem, full path.
ctk load table "file+bson:///path/to/mongodb-json-files/datasets/books.json"

# Extended JSON, HTTP resource.
ctk load table "https+bson://github.com/ozlerhakan/mongodb-json-files/raw/master/datasets/books.json"

# BSON, filesystem, relative path, compressed.
ctk load table "file+bson:./var/data/testdrive/books.bson.gz"

# Extended JSON, filesystem, multiple files.
ctk load table \
  "file+bson:///path/to/mongodb-json-files/datasets/*.json?batch-size=2500" \
  --cratedb-sqlalchemy-url="crate://crate@localhost:4200/datasets"
```
:::{important}
When transferring **multiple collections**, make sure to use a CrateDB database
address which DOES NOT reference an individual table.
It MUST stop at the **schema** label, here, `datasets`. Likewise, the path to
the MongoDB JSON files also MUST reference the **parent folder**, NOT a specific
JSON or BSON file.
:::

To exercise a full example importing multiple MongoDB Extended JSON files,
see [](#file-import-tutorial).


## Options

### Batch Size
The default batch size is 100, but for many datasets a much larger batch size
is applicable for most efficient data transfers. You can adjust the value by
appending the HTTP URL query parameter `batch-size` to the source URL, like
`mongodb+srv://managed.mongodb.net/ticker/stocks?batch-size=5000`.

### Filter
Use the HTTP URL query parameter `filter` on the source URL, like
`&filter={"exchange":{"$eq":"NASDAQ"}}`, or
`&filter={"_id":"66f0002e98c00fb8261d87c8"}`,
in order to provide a MongoDB query filter as a JSON string.
It works in the same way like `mongoexport`'s `--query` option.
On more complex query expressions, make sure to properly encode the right
value using URL/Percent Encoding.

### Limit
Use the HTTP URL query parameter `limit` on the source URL, like
`&limit=100`, in order to limit processing to a total number of
records.

### Offset
Use the HTTP URL query parameter `offset` on the source URL, like
`&offset=42`, in order to start processing at this record from the
beginning.

## Transformations
You can use [Zyp Transformations] to change the shape of the data while being
transferred. In order to add it to the pipeline, use the `--transformation`
command line option.

It is also available for the `migr8 extract` and `migr8 export` commands.
Example transformation files in YAML format can be explored at [examples/zyp].


## Appendix

### Insert Exercise
Import two data points into MongoDB database `testdrive` and collection `demo`,
using the `mongosh` CLI program.
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

(file-import-tutorial)=
### File Import Tutorial

The [mongodb-json-files] repository includes a few samples worth of data in
MongoDB JSON/BSON format.

:::{rubric} Load
:::
Acquire a copy of the repository.
```shell
git clone https://github.com/ozlerhakan/mongodb-json-files.git
```
The data import uses a Zyp project file [zyp-mongodb-json-files.yaml] that
describes a few adjustments needed to import all files flawlessly. Let's
acquire that file.
```shell
wget https://github.com/crate/cratedb-toolkit/raw/v0.0.22/examples/zyp/zyp-mongodb-json-files.yaml
```
Load all referenced `.json` files into corresponding tables within the CrateDB
schema `datasets`, using a batch size of 2,500 items.
```shell
ctk load table \
  "file+bson:///path/to/mongodb-json-files/datasets/*.json?batch-size=2500" \
  --cratedb-sqlalchemy-url="crate://crate@localhost:4200/datasets" \
  --transformation zyp-mongodb-json-files.yaml
```

:::{rubric} Query
:::
After importing the example files, you may want to exercise those SQL queries,
for example using Admin UI or crash, to get an idea about how to work with
CrateDB SQL.

**books.json**
```sql
SELECT 
    data['title'] AS title, 
    LEFT(data['shortDescription'], 60) AS description, 
    DATE_FORMAT('%Y-%m-%d', data['publishedDate']) AS date,
    data['isbn'] AS isbn
FROM datasets.books 
WHERE 'Java' = ANY(data['categories'])
ORDER BY title;
```

**city_inspections.json**
```sql
SELECT
    data['sector'] AS sector, 
    data['business_name'] AS name
FROM datasets.city_inspections
WHERE 
    data['result'] = 'Violation Issued' AND 
    UPPER(data['address']['city']) = 'STATEN ISLAND'
ORDER BY sector, name;
```

:::{tip}
Alternatively, have a look at the canonical
MongoDB C driver's [libbson test files].
:::

## Troubleshooting
When importing from a BSON file, and observing a traceback like this,
```python
Traceback (most recent call last):
  File "/path/to/site-packages/bson/__init__.py", line 1356, in decode_file_iter
    yield _bson_to_dict(elements, opts)  # type:ignore[type-var, arg-type, misc]
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
bson.errors.InvalidBSON: not enough data for a BSON document
```
please try to import the file into a MongoDB Server using `mongorestore`,
and export it again using `mongodump` or `mongoexport`, preferably using
recent versions like MongoDB 7 and tools version 100.9.5 or higher.



[BSON]: https://en.wikipedia.org/wiki/BSON
[examples/zyp]: https://github.com/crate/cratedb-toolkit/tree/main/examples/zyp
[libbson test files]: https://github.com/mongodb/mongo-c-driver/tree/master/src/libbson/tests/json
[MongoDB Extended JSON]: https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/
[mongodb-json-files]: https://github.com/ozlerhakan/mongodb-json-files
[Zyp Transformations]: https://commons-codec.readthedocs.io/zyp/
[zyp-mongodb-json-files.yaml]: https://github.com/crate/cratedb-toolkit/blob/v0.0.22/examples/zyp/zyp-mongodb-json-files.yaml
