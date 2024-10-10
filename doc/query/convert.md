# Query Expression Converter

A very basic query expression converter framework with CLI interface.

## Synopsis
Convert expression using specified converter type.
```shell
ctk query convert --type=<converter-type> <input>
```

## Help
```shell
ctk query convert --help
```

## Converters

## DynamoDB primary key relocator
With CTK 0.0.27, there was a breaking change on the DDL schema where
data from DynamoDB is relayed into.

> DynamoDB: Change CrateDB data model to use (`pk`, `data`, `aux`) columns.
>
> **Attention:** This is a breaking change.

This converter adjusts SQL query expressions to account for that change,
specifically amending primary key object references.

### Usage

Supply query expression via STDIN.
```shell
echo "SELECT * FROM foobar WHERE data['PK']" | \
  ctk query convert --type=ddb-relocate-pks --primary-keys=PK,SK - 
```

Supply query expression via filesystem resource.
```shell
echo "SELECT * FROM foobar WHERE data['PK']" > input.sql
ctk query convert --type=ddb-relocate-pks --primary-keys=PK,SK input.sql > output.sql
cat output.sql
```

Result:
```sql
SELECT * FROM foobar WHERE pk['PK']
```
