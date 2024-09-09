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

  Read [MongoDB Extended JSON] format from filesystem.

## Install
```shell
pip install --upgrade 'cratedb-toolkit[mongodb]'
```

## Usage
The MongoDB I/O adapter can process MongoDB data from different sources.
This section enumerates relevant connectivity options on behalf of
concrete usage examples.

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

### MongoDB Atlas
Transfer data from MongoDB Atlas.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk load table "mongodb+srv://john:EeY6OocooL8rungu@testdrive.ahnaik1.mongodb.net/ticker/stocks?batch-size=5000"
```

### MongoDB JSON/BSON files
Load data from MongoDB JSON/BSON files, for example produced by the
`mongoexport` or `mongodump` programs.
In order to get hold of a few samples worth of data, the canonical MongoDB C
driver's [libbson test files] includes a few. In this case, let's acquire
the collection at [mongodb-json-files].
```shell
git clone https://github.com/ozlerhakan/mongodb-json-files.git
```
```shell
CRATEDB_SQLALCHEMY_BASEURL=crate://crate@localhost:4200/testdrive
ctk load table \
  "file+bson:///path/to/mongodb-json-files/datasets/books.json" \
  --cratedb-sqlalchemy-url="${CRATEDB_SQLALCHEMY_BASEURL}/books"
```
Address relative and/or compressed BSON files like
`file+bson:./tmp/testdrive/books.bson.gz`.

Example queries that fit the schema of `books.json`, and more, can be
found at [](#ready-made-queries).


## Options

### Batch Size
The default batch size is 500. You can adjust the value by appending the HTTP
URL query parameter `batch-size` to the source URL, like
`mongodb+srv://managed.mongodb.net/ticker/stocks?batch-size=5000`.

### Offset
Use the HTTP URL query parameter `offset` on the source URL, like
`&offset=42`, in order to start processing at this record from the
beginning.

### Limit
Use the HTTP URL query parameter `limit` on the source URL, like
`&limit=100`, in order to limit processing to a total number of
records.

## Zyp Transformations
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

(ready-made-queries)=
### Ready-Made Queries
The [mongodb-json-files] repository includes a few samples worth of data in MongoDB
JSON/BSON format. After importing them, you may want to exercise those SQL queries,
for example using Admin UI or crash.

#### books.json
```sql
SELECT 
    data['title'] AS title, 
    LEFT(data['shortDescription'], 60) AS description, 
    DATE_FORMAT('%Y-%m-%d', data['publishedDate']) AS date,
    data['isbn'] AS isbn
FROM testdrive.books 
WHERE 'Java' = ANY(data['categories'])
ORDER BY title;
```

#### city_inspections.json
```sql
SELECT
    data['sector'] AS sector, 
    data['business_name'] AS name
FROM testdrive.city_inspections
WHERE 
    data['result'] = 'Violation Issued' AND 
    UPPER(data['address']['city']) = 'STATEN ISLAND'
ORDER BY sector, name;
```

### Backlog
:::{todo}
- Describe usage of `mongoimport` and `mongoexport`.
  ```shell
  mongoimport --uri 'mongodb+srv://MYUSERNAME:SECRETPASSWORD@mycluster-ABCDE.azure.mongodb.net/test?retryWrites=true&w=majority'
  ```
- Convert dates like `"date": "Sep 18 2015"`, see `testdrive.city_inspections`.
:::


[examples/zyp]: https://github.com/crate/cratedb-toolkit/tree/main/examples/zyp
[libbson test files]: https://github.com/mongodb/mongo-c-driver/tree/master/src/libbson/tests/json
[MongoDB Extended JSON]: https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/
[mongodb-json-files]: https://github.com/ozlerhakan/mongodb-json-files
[Zyp Transformations]: https://commons-codec.readthedocs.io/zyp/index.html
