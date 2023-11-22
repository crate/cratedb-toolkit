# PyMongo CrateDB Adapter

## Iteration +1

Make it work.

- Using individual columns for fields does not work, because `insert_one` works
  iteratively, and needs to evolve the table schema gradually. As a consequence,
  we need to use `OBJECT(DYNAMIC)` for storing MongoDB fields.
- Add software tests
- Add documentation

## Iteration +2

Translate query expressions.

- https://github.com/gordonbusyman/mongo-to-sql-converter
- https://github.com/2do2go/json-sql

- https://github.com/SY-Xuan/mongo2sql
- https://github.com/nsragow/MongoToSqlParse
- https://github.com/sushmitharao2124/MongoToSQLConverter
- https://github.com/Phomint/MongoSQL
