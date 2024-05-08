(migr8)=
# migr8

## About

A utility program, called `migr8`, supporting data migrations
between MongoDB and CrateDB.

:::{tip}
Please also visit the documentation about the [](#mongodb-loader)
to learn about a more high-level interface.
:::

### Details

This tool iterates over one or multiple MongoDB collections,
and iteratively builds up a description of the schema of those
collections.

In a second step, this description can be used to create a CrateDB table
schema, which will attempt to determine a best-fit table definition for
that schema.

As such, this means the tool works best on collections of similarly
structured and typed data.

### Supported MongoDB versions

The application supports the following versions of MongoDB.

[![Supported MongoDB versions](https://img.shields.io/badge/MongoDB-2.x%20--%207.x-blue.svg)](https://github.com/mongodb/mongo)

If you need support for MongoDB 2.x, you will need to downgrade the `pymongo`
client driver library to version 3, like `pip install 'pymongo<4'`.

### Installation

Use `pip` to install the package from PyPI.
```shell
pip install --upgrade 'cratedb-toolkit[mongodb]'
```

To verify if the installation worked, invoke:
```shell
ctk --version
```


## Usage

`ctk load table` is your one-stop command to populate a CrateDB table from a
MongoDB collection.

```shell
export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
ctk load table mongodb://localhost:27017/testdrive/demo
```

It will run `extract` and `translate` to gather the SQL DDL schema, and will
invoke `export` and `cr8` to actually transfer data.


## Usage for `migr8`

The program `migr8` offers three subcommands (`extract`, `translate`, `export`),
to conclude data transfers from MongoDB to CrateDB. Please read this section
carefully to learn how they can be used successfully.

```shell
migr8 --version
migr8 --help
```

### Schema Extraction

To extract a description of the schema of a collection, use the
`extract` subcommand. For example:

    migr8 extract --host localhost --port 27017 --database test_db

After connecting to the designated MongoDB server, it will
look at the collections within that database, and will prompt you which
collections to *exclude* from analysis.

You can then do a *full* or *partial* scan of the collection.

A partial scan will only look at the first entry in a collection, and
thus may produce an ambiguous schema definition. It is still useful if you
already know the collection is systematically and regularly structured.

A full scan will iterate over the entire collection and build up the
schema description. Cancelling the scan will cause the tool to output
the schema description it has built up thus far.

For example, scanning a collection of payloads including a `ts` field,
a `sensor` field, and a `payload` object, may yield this outcome:

```json
{
  "test": {
    "count": 100000,
    "document": {
      "_id": {
        "count": 100000,
        "types": {
          "OID": {
            "count": 100000
          }
        }
      },
      "ts": {
        "count": 100000,
        "types": {
          "DATETIME": {
            "count": 100000
          }
        }
      },
      "sensor": {
        "count": 100000,
        "types": {
          "STRING": {
            "count": 100000
          }
        }
      },
      "payload": {
        "count": 100000,
        "types": {
          "OBJECT": {
            "count": 100000,
            "document": {
              "temp": {
                "count": 100000,
                "types": {
                  "FLOAT": {
                    "count": 1
                  },
                  "INTEGER": {
                    "count": 99999
                  }
                }
              },
              "humidity": {
                "count": 100000,
                "types": {
                  "FLOAT": {
                    "count": 1
                  },
                  "INTEGER": {
                    "count": 99999
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

This description indicates that the data is well-structured, and has
mostly consistent data-types.


### Schema Translation

Once a schema description has been extracted, it can be translated
into a CrateDB schema definition using the `translate` subcommand:

    migr8 translate -i mongodb_schema.json

This will attempt to translate the description into a best-fit CrateDB
table definition. Where datatypes are ambiguous, it will *choose the
most common datatype*. For example, the previous schema definition would
be translated into this SQL DDL statement:
```sql
CREATE TABLE IF NOT EXISTS "doc"."test" (
    "ts" TIMESTAMP WITH TIME ZONE,
    "sensor" TEXT,
    "payload" OBJECT (STRICT) AS (
        -- ⬇️ Types: FLOAT: 0.0%, INTEGER: 100.0%
        "temp" INTEGER,
        -- ⬇️ Types: FLOAT: 0.0%, INTEGER: 100.0%
        "humidity" INTEGER
    )
);
```


### MongoDB Collection Export

To export a MongoDB collection to a JSON stream, use the `export`
subcommand:

    migr8 export --host localhost --port 27017 --database test_db --collection test

This will convert the collection's records into JSON, and output the JSON to stdout.
For example, to redirect the output to a file, run:

    migr8 export --host localhost --port 27017 --database test_db --collection test > test.json

Alternatively, use [cr8] to directly write the MongoDB collection into a CrateDB table:

    migr8 export --host localhost --port 27017 --database test_db --collection test | \
        cr8 insert-json --hosts localhost:4200 --table test


[cr8]: https://github.com/mfussenegger/cr8
