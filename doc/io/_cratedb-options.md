:::{rubric} CrateDB options
:::

Please make sure to replace username, password, and
hostname with values matching your environment.

- `ssl`: Use the `?ssl=true` query parameter to enable SSL. Also use this when
  connecting to CrateDB Cloud.
  ```text
  'crate://crate:crate@cratedb.example.org:4200/schema/table?ssl=true'
  ```
