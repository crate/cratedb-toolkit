(motherduck)=

# MotherDuck

:::{div} sd-text-muted
Load data from MotherDuck into CrateDB.
:::

[MotherDuck] -- The cloud data warehouse built for answers, in SQL or natural language.

```shell
ctk load \
    "motherduck://<database-name>?token=<your-token>&table=<schema-name>.<table-name>" \
    "crate://crate:na@localhost:4200/testdrive/motherduck"
```


[MotherDuck]: https://motherduck.com/
