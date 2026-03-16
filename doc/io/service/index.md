(io-service)=

# Services

:::{div} sd-text-muted
Import data from APIs and services.
:::

:::{include} ../_install-ingest.md
:::

## Integrations

:::::{grid} 2 3 3 4
:gutter: 2
:padding: 0

::::{grid-item-card}
:link: airtable
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/airtable.svg
:height: 80px
:alt:
```
+++
Airtable
::::

::::{grid-item-card}
:link: anthropic
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/anthropic.svg
:height: 80px
:alt:
```
+++
Anthropic
::::

::::{grid-item-card}
:link: asana
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/asana.svg
:height: 80px
:alt:
```
+++
Asana
::::

::::{grid-item-card}
:link: attio
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/attio.svg
:height: 80px
:alt:
```
+++
Attio
::::

::::{grid-item-card}
:link: facebook-ads
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/facebook.svg
:height: 80px
:alt:
```
+++
Facebook Ads
::::

::::{grid-item-card}
:link: github
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/github.svg
:height: 80px
:alt:
```
+++
GitHub
::::

::::{grid-item-card}
:link: google-ads
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/google-ads.svg
:height: 80px
:alt:
```
+++
Google Ads
::::

::::{grid-item-card}
:link: google-analytics
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/google-analytics.svg
:height: 80px
:alt:
```
+++
Google Analytics
::::

::::{grid-item-card}
:link: google-sheets
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/google-sheets.svg
:height: 80px
:alt:
```
+++
Google Sheets
::::

::::{grid-item-card}
:link: hubspot
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/hubspot.svg
:height: 80px
:alt:
```
+++
HubSpot
::::

::::{grid-item-card}
:link: jira
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/jira.svg
:height: 80px
:alt:
```
+++
Jira
::::

::::{grid-item-card}
:link: salesforce
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/salesforce.svg
:height: 80px
:alt:
```
+++
Salesforce
::::

::::{grid-item-card}
:link: shopify
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/shopify.svg
:height: 80px
:alt:
```
+++
Shopify
::::

::::{grid-item-card}
:link: slack
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/slack.svg
:height: 80px
:alt:
```
+++
Slack
::::

::::{grid-item-card}
:link: stripe
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/stripe.svg
:height: 80px
:alt:
```
+++
Stripe
::::

::::{grid-item-card}
:link: wise
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/wise.svg
:height: 80px
:alt:
```
+++
Wise
::::

::::{grid-item-card}
:link: zendesk
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/zendesk.svg
:height: 80px
:alt:
```
+++
Zendesk
::::

::::{grid-item-card}
:link: zoom
:link-type: ref
:class-item: sd-fs-1 sd-text-center
:class-footer: sd-fs-5 sd-font-weight-bold
```{image} /_static/logo/zoom.svg
:height: 80px
:alt:
```
+++
Zoom
::::

:::::

## Examples

(airtable)=
:::{rubric} Airtable
:::
Load data from Airtable into CrateDB.
```shell
ctk load \
    "airtable://?access_token=<access_token>&table=demo" \
    "crate://crate:na@localhost:4200/testdrive/airtable"
```

(anthropic)=
:::{rubric} Anthropic
:::
Load comprehensive data from the Anthropic Admin API into CrateDB,
including Claude Code usage metrics, API usage reports, cost data,
and organization management information.
See [Anthropic entities] about any labels you can use for the
`table` parameter in the source URL.
```shell
ctk load \
    "anthropic://?api_key=<admin_api_key>&table=claude_code_usage" \
    "crate://crate:na@localhost:4200/testdrive/anthropic_claude_code_usage"
```

(asana)=
:::{rubric} Asana
:::
Load data from Asana into CrateDB.
See [Asana entities] about any labels you can use for the
`table` parameter in the source URL.
```shell
ctk load \
    "asana://<workspace_id>?access_token=<access_token>&table=workspaces" \
    "crate://crate:na@localhost:4200/testdrive/asana_workspaces"
```

(attio)=
:::{rubric} Attio
:::
Load data from Attio into CrateDB.
See [Attio entities] about any labels you can use for the
`table` parameter in the source URL.
```shell
ctk load \
    "attio://?api_key=<api_key>&table=objects" \
    "crate://crate:na@localhost:4200/testdrive/attio_objects"
```

(facebook-ads)=
:::{rubric} Facebook Ads
:::
Load data from Facebook Ads into CrateDB.
See [Facebook Ads entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load \
    "facebookads://?access_token=<access_token>&account_id=<account_id>&table=campaigns" \
    "crate://crate:na@localhost:4200/testdrive/facebookads_campaigns"
```

(github)=
:::{rubric} GitHub
:::
Load data from GitHub into CrateDB.
See [GitHub entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load \
    "github://?access_token=${GH_TOKEN}&owner=crate&repo=cratedb-toolkit&table=issues" \
    "crate://crate:na@localhost:4200/testdrive/github_issues"
```

(google-ads)=
:::{rubric} Google Ads
:::
Load data from Google Ads into CrateDB.
See [Google Ads entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load \
    "googleads://<customer_id>?credentials_path=/path/to/service-account.json&dev_token=<dev_token>&table=campaign_report_daily" \
    "crate://crate:na@localhost:4200/testdrive/googleads_campaign_report_daily"
```

(google-analytics)=
:::{rubric} Google Analytics
:::
Load data from Google Analytics into CrateDB.
See [Google Analytics entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load \
    "googleanalytics://?credentials_path=/path/to/service/account.json&property_id=<property_id>&table=realtime" \
    "crate://crate:na@localhost:4200/testdrive/googleanalytics_realtime"
```
```shell
ctk load \
    "googleanalytics://?credentials_base64=<base64_encoded_credentials>&property_id=<property_id>&table=realtime" \
    "crate://crate:na@localhost:4200/testdrive/googleanalytics_realtime"
```

(google-sheets)=
:::{rubric} Google Sheets
:::
Load data from Google Sheets into CrateDB.
```shell
ctk load \
    "gsheets://?credentials_path=/path/to/service/account.json&table=fkdUQ2bjdNfUq2CA.Sheet1" \
    "crate://crate:na@localhost:4200/testdrive/gsheets"
```

(hubspot)=
:::{rubric} HubSpot
:::
Load data from HubSpot into CrateDB.
See [HubSpot entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load \
    "hubspot://?api_key=<api-key-here>&table=deals" \
    "crate://crate:na@localhost:4200/testdrive/hubspot_deals"
```

(jira)=
:::{rubric} Jira
:::
Load data from Jira into CrateDB.
See [Jira entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load \
    "jira://your-domain.atlassian.net?email=<email>&api_token=<api_token>&table=issues" \
    "crate://crate:na@localhost:4200/testdrive/jira_issues"
```

(salesforce)=
:::{rubric} Salesforce
:::
Load data from Salesforce into CrateDB.
See [Salesforce entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load \
    "salesforce://?username=<username>&password=<password>&token=<token>&table=opportunity" \
    "crate://crate:na@localhost:4200/testdrive/salesforce_opportunity"
```

(shopify)=
:::{rubric} Shopify
:::
Load data from Shopify into CrateDB.
See [Shopify entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load \
    "shopify://<shopify store URL>?api_key=token&table=orders" \
    "crate://crate:na@localhost:4200/testdrive/shopify_orders"
```

(slack)=
:::{rubric} Slack
:::
Load data from Slack into CrateDB.
See [Slack entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load \
    "slack://?api_key=${SLACK_TOKEN}&table=channels" \
    "crate://crate:na@localhost:4200/testdrive/slack_channels"
```

(stripe)=
:::{rubric} Stripe
:::
Load data from Stripe into CrateDB.
See [Stripe entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load \
    "stripe://?api_key=${STRIPE_API_KEY}&table=charge" \
    "crate://crate:na@localhost:4200/testdrive/stripe_charges"
```

(wise)=
:::{rubric} Wise
:::
Load data from Wise into CrateDB.
See [Wise entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load \
    "wise://?api_key=<api_token>&table=transfers" \
    "crate://crate:na@localhost:4200/testdrive/wise_transfers"
```

(zendesk)=
:::{rubric} Zendesk
:::
Load data from Zendesk into CrateDB.
See [Zendesk entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load \
    "zendesk://:<oauth_token>@<sub-domain>?table=tickets" \
    "crate://crate:na@localhost:4200/testdrive/zendesk_tickets"
```

(zoom)=
:::{rubric} Zoom
:::
Load data from Zoom into CrateDB.
See [Zoom entities] about any labels you can use for the `table` parameter
in the source URL.
```shell
ctk load \
    "zoom://?client_id=<client_id>&client_secret=<client_secret>&account_id=<account_id>&table=meetings" \
    "crate://crate:na@localhost:4200/testdrive/zoom_meetings"
```


[Anthropic entities]: https://bruin-data.github.io/ingestr/supported-sources/anthropic.html#available-tables
[Asana entities]: https://bruin-data.github.io/ingestr/supported-sources/asana.html#tables
[Attio entities]: https://bruin-data.github.io/ingestr/supported-sources/attio.html#tables
[Facebook Ads entities]: https://bruin-data.github.io/ingestr/supported-sources/facebook-ads.html#tables
[GitHub entities]: https://bruin-data.github.io/ingestr/supported-sources/github.html#tables
[Google Ads entities]: https://bruin-data.github.io/ingestr/supported-sources/google-ads.html#tables
[Google Analytics entities]: https://bruin-data.github.io/ingestr/supported-sources/google_analytics.html#available-tables
[HubSpot entities]: https://bruin-data.github.io/ingestr/supported-sources/hubspot.html#tables
[Jira entities]: https://bruin-data.github.io/ingestr/supported-sources/jira.html#tables
[Salesforce entities]: https://bruin-data.github.io/ingestr/supported-sources/salesforce.html#tables
[Shopify entities]: https://bruin-data.github.io/ingestr/supported-sources/shopify.html#tables
[Slack entities]: https://bruin-data.github.io/ingestr/supported-sources/slack.html#tables
[Stripe entities]: https://bruin-data.github.io/ingestr/supported-sources/stripe.html#all-endpoints
[Wise entities]: https://bruin-data.github.io/ingestr/supported-sources/wise.html#tables
[Zendesk entities]: https://bruin-data.github.io/ingestr/supported-sources/zendesk.html#tables
[Zoom entities]: https://bruin-data.github.io/ingestr/supported-sources/zoom.html#uri-format
