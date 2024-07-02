# Rockset API Emulator Backlog

## Iteration +1
- Properly relay error messages like `RelationUnknown[Relation 'commons.foobar2' unknown. Maybe you meant 'foobar']`
  instead of `Internal Server Error`
- Populate the `"collections":["UNKNOWN"]` response field on the `query` API.
- Emulate the "Collections" API, at least provide a no-op.
- Software integration tests, including client examples.
- /v1/telemetry is used by Rockset CLI
- Create a Collection (With Field Mappings)
  https://github.com/rockset/rockset-js/tree/master/packages/cli#collections

## Iteration +2
- When using uppercase characters in table names (workspace/collection),
  they are not properly quoted like CrateDB needs it.
- InvalidColumnNameException["_event_time" conflicts with system column pattern]
- Implement "ValidateQuery"
- Implement "AddDocumentsWithOffset". See Golang example.
- Implement pagination.

## Iteration +3
- Tap into datasets
  https://github.com/rockset/rockset-go-client/tree/master/dataset
- Integrations?
