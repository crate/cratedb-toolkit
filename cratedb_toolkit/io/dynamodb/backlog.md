# DynamoDB Backlog

## Iteration +1
- Pagination / Batch Getting.
  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/programming-with-python.html#programming-with-python-pagination

- Use `batch_get_item`.
  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/batch_get_item.html

- Scan by query instead of full.


## Iteration +2

### Resumption on errors?
Another variant to scan the table, probably for resuming on errors?
```python
key = None
while True:
  if key is None:
    response = table.scan()
  else:
    response = table.scan(ExclusiveStartKey=key)
  key = response.get("LastEvaluatedKey", None)
```

### Item transformations?
That's another item transformation idea picked up from an example program.
Please advise if this is sensible in all situations, or if it's just a
special case.

```python
if 'id' in item and not isinstance(item['id'], str):
    item['id'] = str(item['id'])
```
