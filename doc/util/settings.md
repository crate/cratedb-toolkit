(setting)=
(settings)=
# ctk settings

CrateDB Toolkit's "Settings" subsystem provides tools to list and compare default
vs. runtime settings.

## Install
```shell
uv pip install 'cratedb-toolkit[settings]'
```

## Usage

### List default settings

This tool extracts [settings] from CrateDB's documentation and outputs them
in either JSON, YAML or Markdown formats, or SQL statements to set the default value.

It parses the HTML structure of the documentation to identify settings, their
descriptions, default values, and whether they are runtime-configurable or not.

```shell
ctk settings list
```

### Compare settings

Compare CrateDB cluster settings against default values.

Acquire default settings from CrateDB's documentation and runtime settings
from a CrateDB cluster and compare them against each other.

Also handles memory and time-based settings with appropriate tolerances.

```shell
export CRATEDB_CLUSTER_URL=crate://crate@localhost:4200/
ctk settings compare
```

:::{rubric} Example output
:::
![image](https://github.com/user-attachments/assets/82d1f99f-70bc-4401-9c03-43731a73418c)


:::{tip}
Append the `--help` command-line flag to inquire available CLI options.
```shell
ctk settings {list,compare} --help
```
:::


[settings]: https://cratedb.com/docs/crate/reference/en/latest/config/cluster.html
