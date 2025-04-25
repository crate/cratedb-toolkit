# Docs API

CrateDB Toolkit's Docs API provides programmatic access to CrateDB's documentation.

## Install
```shell
uv pip install 'cratedb-toolkit[docs-api]'
```

## Usage

### CrateDB functions

This tool extracts [scalar functions] from CrateDB's documentation and outputs them
in either JSON, YAML or Markdown formats.

```shell
ctk docs functions --help
```

:::{rubric} Example
:::
```shell
ctk docs functions --format=json
```
```json
{
  "meta": {
    "created": "2025-04-14T10:53:36.943924",
    "generator": "CrateDB Toolkit",
    "source_url": "https://github.com/crate/crate/raw/refs/heads/5.10/docs/general/builtins/scalar-functions.rst"
  },
  "functions": {
    "concat('first_arg', second_arg, [ parameter , ... ])": {
      "name": "concat",
      "signature": "concat('first_arg', second_arg, [ parameter , ... ])",
      "category": "String functions",
      "description": "Concatenates a variable number of arguments into a single string. It ignores\nNULL values.",
      "returns": null,
      "example": null
    },
    "concat_ws('separator', second_arg, [ parameter , ... ])": {
      "name": "concat_ws",
      "signature": "concat_ws('separator', second_arg, [ parameter , ... ])",
      "category": "String functions",
      "description": "Concatenates a variable number of arguments into a single string using a\nseparator defined by the first argument. If first argument is NULL the\nreturn value is NULL. Remaining NULL arguments are ignored.",
      "returns": null,
      "example": null
    }
  }
}
```

### CrateDB settings

This tool extracts [settings] from CrateDB's documentation and outputs them
in either JSON, YAML or Markdown formats, or SQL statements to set the default value.

It parses the HTML structure of the documentation to identify settings, their
descriptions, default values, and whether they are runtime-configurable or not.

```shell
ctk docs settings --help
```

:::{rubric} Example
:::
```shell
ctk docs settings --format=json
```
```json
{
  "meta": {
    "created": "2025-04-14T10:52:49.201344",
    "generator": "CrateDB Toolkit",
    "source_url": "https://cratedb.com/docs/crate/reference/en/latest/config/cluster.html"
  },
  "settings": {
    "stats.enabled": {
      "raw_description": "Default: true\nRuntime: yes\n\nA boolean indicating whether or not to collect statistical information about\nthe cluster.\n\nCaution\nThe collection of statistical information incurs a slight performance\npenalty, as details about every job and operation across the cluster will\ncause data to be inserted into the corresponding system tables.",
      "runtime_configurable": true,
      "default_value": "true",
      "type": "",
      "purpose": "Default: true\n\n\nA boolean indicating whether or not to collect statistical information about\nthe cluster. Caution\nThe collection of statistical information incurs a slight performance\npenalty, as details about every job and operation across the cluster will\ncause data to be inserted into the corresponding system tables.",
      "constraints": "",
      "related_settings": [],
      "deprecated": false,
      "stmt": "SET GLOBAL PERSISTENT \"stats.enabled\" = 'true'"
    },
    "stats.jobs_log_size": {
      "raw_description": "Default: 10000\nRuntime: yes\n\nThe maximum number of job records kept to be kept in the sys.jobs_log table on each node.\nA job record corresponds to a single SQL statement to be executed on the\ncluster. These records are used for performance analytics. A larger job log\nproduces more comprehensive stats, but uses more RAM.\nOlder job records are deleted as newer records are added, once the limit is\nreached.\nSetting this value to 0 disables collecting job information.",
      "runtime_configurable": true,
      "default_value": "10000",
      "type": "",
      "purpose": "Default: 10000\n\n\nThe maximum number of job records kept to be kept in the sys.jobs_log table on each node. A job record corresponds to a single SQL statement to be executed on the\ncluster.",
      "constraints": "maximum number of job records kept to be kept in the sys.jobs_log table on each node.",
      "related_settings": [
        "sys.jobs_log"
      ],
      "deprecated": false,
      "stmt": "SET GLOBAL PERSISTENT \"stats.jobs_log_size\" = 10000"
    }
  }
}
```


[scalar functions]: https://cratedb.com/docs/crate/reference/en/latest/general/builtins/scalar-functions.html
[settings]: https://cratedb.com/docs/crate/reference/en/latest/config/cluster.html
