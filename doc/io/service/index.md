# Services

:::{div} sd-text-muted
Import data from APIs and services.
:::

:::{include} ../_install-ingest.md
:::

## Integrations

Load data from Salesforce into CrateDB.
```shell
ctk load table \
    "salesforce://?username=<username>&password=<password>&token=<token>&table=opportunity" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/salesforce_opportunity"
```

Load data from GitHub into CrateDB.
```shell
ctk load table \
    "github://?access_token=${GH_TOKEN}&owner=crate&repo=cratedb-toolkit&table=issues" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/github_ctk_issues"
```

Load data from HubSpot into CrateDB.
See [HubSpot entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load table \
    "hubspot://?api_key=<api-key-here>&table=deals" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/hubspot_deals"
```

Load data from Salesforce into CrateDB.
See [Salesforce entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load table \
    "salesforce://?username=<username>&password=<password>&token=<token>&table=opportunity" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/salesforce_opportunity"
```

Load data from Slack into CrateDB.
See [Slack entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load table \
    "slack://?api_key=${SLACK_TOKEN}&table=channels" \
    --cluster-url="crate://crate:na@localhost:4200/testdrive/slack_channels"
```


[HubSpot entities]: https://bruin-data.github.io/ingestr/supported-sources/hubspot.html#tables
[Salesforce entities]: https://bruin-data.github.io/ingestr/supported-sources/salesforce.html#tables
[Slack entities]: https://bruin-data.github.io/ingestr/supported-sources/slack.html#tables
