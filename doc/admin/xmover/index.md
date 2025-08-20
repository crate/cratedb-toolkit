# XMover

:::{div} sd-text-muted
CrateDB Shard Analyzer and Movement Tool.
:::

A comprehensive looking-glass utility for analyzing CrateDB shard
distribution across nodes and availability zones. It generates safe
SQL commands for shard rebalancing and node decommissioning.

## Features

- **Cluster Analysis**: Complete overview of shard distribution across nodes and zones
- **Shard Movement Recommendations**: Intelligent suggestions for rebalancing with safety validation
- **Recovery Monitoring**: Track ongoing shard recovery operations with progress details
- **Zone Conflict Detection**: Prevents moves that would violate CrateDB's zone awareness
- **Node Decommissioning**: Plan safe node removal with automated shard relocation
- **Dry Run Mode**: Test recommendations without generating actual SQL commands
- **Safety Validation**: Comprehensive checks to ensure data availability during moves

## Documentation

```{toctree}
:maxdepth: 1

Handbook <handbook>
Troubleshooting <troubleshooting>
Query Gallery <queries>
```
