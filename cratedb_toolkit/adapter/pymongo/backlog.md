# PyMongo CrateDB Adapter Backlog

## Iteration +1
- Upstream / converge patches.
  - `cratedb_toolkit/util/sqlalchemy.py`
  - `cratedb_toolkit/util/pandas.py`
  - `cratedb_toolkit/adapter/pymongo/api.py::adjust_sqlalchemy`
  - `cratedb_toolkit/adapter/pymongo/collection.py::insert_returning_id`
- Make `jessiql` work with more recent SQLAlchemy 2.x.
- Make adapter work with pandas 2.2.

## Iteration +2
- Add documentation.
- Add missing essential querying features: Examples: sort order, skip, limit
- Add missing essential methods. Example: `db.my_collection.drop()`.

## Iteration +2
- Make write-synchronization behavior (refresh table) configurable.
- Handle deeply nested documents of various types.
- Unlock more features from canonical examples.
  https://pymongo.readthedocs.io/en/stable/examples/

## Iteration +3
- Decoding timestamps are yielding only Decimals instead of datetime objects
  when printed on the terminal? See example program.
  There is also a warning:
  ```shell
  SAWarning: Dialect crate+crate-python does *not* support Decimal objects natively,
  and SQLAlchemy must convert from floating point - rounding errors and other issues
  may occur. Please consider storing Decimal numbers as strings or integers on this
  platform for lossless storage.
  ```
  So, why are `Decimal` types used here, at all?
- Mimic MongoDB exceptions.
  Example:
  ```python
  jessiql.exc.InvalidColumnError: Invalid column "x" for "Surrogate" specified in sort
  ```


## Done

- Make it work
- Using individual columns for fields does not work, because `insert_one` works
  iteratively, and needs to evolve the table schema gradually. As a consequence,
  we need to use `OBJECT(DYNAMIC)` for storing MongoDB fields.
- Add software tests

### Research
How to translate a MongoDB query expression?

- https://github.com/gordonbusyman/mongo-to-sql-converter
- https://github.com/2do2go/json-sql

- https://github.com/kolypto/py-mongosql
- https://github.com/SY-Xuan/mongo2sql
- https://github.com/nsragow/MongoToSqlParse
- https://github.com/sushmitharao2124/MongoToSQLConverter
- https://github.com/Phomint/MongoSQL
