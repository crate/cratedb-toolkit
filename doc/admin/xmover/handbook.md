(xmover-handbook)=
# XMover Handbook

## Installation

Install using uv (recommended) or pip:
```bash
uv tool install cratedb-toolkit

# Alternatively use `pip`.
# pip install --user cratedb-toolkit
```

Create an `.env` file with your CrateDB connection details:
```bash
CRATE_CONNECTION_STRING=https://your-cluster.cratedb.net:4200
CRATE_USERNAME=your-username
CRATE_PASSWORD=your-password
CRATE_SSL_VERIFY=true
```

## Quick Start

### Test Connection
```bash
xmover test-connection
```

### Analyze Cluster
```bash
# Complete cluster analysis
xmover analyze

# Analyze specific table
xmover analyze --table my_table
```

### Find Movement Candidates
```bash
# Find shards that can be moved (40-60GB by default)
xmover find-candidates

# Custom size range
xmover find-candidates --min-size 20 --max-size 100
```

### Generate Recommendations
```bash
# Dry run (default) - shows what would be recommended
xmover recommend

# Generate actual SQL commands
xmover recommend --execute

# Prioritize space over zone balancing
xmover recommend --prioritize-space
```

### Shard Distribution Analysis
This view focuses on large tables.
```bash
# Analyze distribution anomalies for top 10 largest tables
xmover shard-distribution

# Analyze more tables
xmover shard-distribution --top-tables 20

# Detailed health report for specific table
xmover shard-distribution --table my_table
```

### Zone Analysis
```bash
# Check zone balance
xmover check-balance

# Detailed zone analysis with shard-level details
xmover zone-analysis --show-shards
```

### Advanced Troubleshooting
```bash
# Validate specific moves before execution
xmover validate-move SCHEMA.TABLE SHARD_ID FROM_NODE TO_NODE

# Explain CrateDB error messages
xmover explain-error "your error message here"
```

## Commands Reference

### `analyze`
Analyzes current shard distribution across nodes and zones.

**Options:**
- `--table, -t`: Analyze specific table only

**Example:**
```bash
xmover analyze --table events
```

### `find-candidates`
Finds shards suitable for movement based on size and health criteria.

**Options:**
- `--table, -t`: Find candidates in specific table only
- `--min-size`: Minimum shard size in GB (default: 40)
- `--max-size`: Maximum shard size in GB (default: 60)
- `--node`: Only show candidates from this specific source node (e.g., data-hot-4)

**Examples:**
```bash
# Find candidates in size range for specific table
xmover find-candidates --min-size 20 --max-size 50 --table logs

# Find candidates on a specific node
xmover find-candidates --min-size 30 --max-size 60 --node data-hot-4
```

### `recommend`
Generates intelligent shard movement recommendations for cluster rebalancing.

**Options:**
- `--table, -t`: Generate recommendations for specific table only
- `--min-size`: Minimum shard size in GB (default: 40)
- `--max-size`: Maximum shard size in GB (default: 60)
- `--zone-tolerance`: Zone balance tolerance percentage (default: 10)
- `--min-free-space`: Minimum free space required on target nodes in GB (default: 100)
- `--max-moves`: Maximum number of move recommendations (default: 10)
- `--max-disk-usage`: Maximum disk usage percentage for target nodes (default: 90)
- `--validate/--no-validate`: Validate move safety (default: True)
- `--prioritize-space/--prioritize-zones`: Prioritize available space over zone balancing (default: False)
- `--dry-run/--execute`: Show what would be done without generating SQL commands (default: True)
- `--node`: Only recommend moves from this specific source node (e.g., data-hot-4)
- `--auto-execute`: Automatically execute the SQL commands (requires `--execute`, asks for confirmation) (default: False)

**Examples:**
```bash
# Dry run with zone balancing priority
xmover recommend --prioritize-zones

# Generate SQL for space optimization
xmover recommend --prioritize-space --execute

# Focus on specific table with custom parameters
xmover recommend --table events --min-size 10 --max-size 30 --execute

# Target space relief for a specific node
xmover recommend --prioritize-space --min-size 30 --max-size 60 --node data-hot-4

# Allow higher disk usage for urgent moves
xmover recommend --prioritize-space --max-disk-usage 90
```

### `zone-analysis`
Provides detailed analysis of zone distribution and potential conflicts.

**Options:**
- `--table, -t`: Analyze zones for specific table only
- `--show-shards/--no-show-shards`: Show individual shard details (default: False)

**Example:**
```bash
xmover zone-analysis --show-shards --table critical_data
```

### `check-balance`
Checks zone balance for shards with configurable tolerance.

**Options:**
- `--table, -t`: Check balance for specific table only
- `--tolerance`: Zone balance tolerance percentage (default: 10)

**Example:**
```bash
xmover check-balance --tolerance 15
```



### `validate-move`
Validates a specific shard move before execution to prevent errors.

**Arguments:**
- `SCHEMA_TABLE`: Schema and table name (format: schema.table)
- `SHARD_ID`: Shard ID to move
- `FROM_NODE`: Source node name
- `TO_NODE`: Target node name

**Examples:**
```bash
# Standard validation
xmover validate-move CUROV.maddoxxxS 4 data-hot-1 data-hot-3

# Allow higher disk usage for urgent moves
xmover validate-move CUROV.tendedero 4 data-hot-1 data-hot-3 --max-disk-usage 90
```

### `explain-error`
Explains CrateDB allocation error messages and provides troubleshooting guidance.

**Arguments:**
- `ERROR_MESSAGE`: The CrateDB error message to analyze (optional - can be provided interactively)

**Examples:**
```bash
# Interactive mode
xmover explain-error

# Direct analysis
xmover explain-error "NO(a copy of this shard is already allocated to this node)"
```

### `monitor-recovery`
Monitors active shard recovery operations on the cluster.

**Options:**
- `--table, -t`: Monitor recovery for specific table only
- `--node, -n`: Monitor recovery on specific node only
- `--watch, -w`: Continuously monitor (refresh every 10s)
- `--refresh-interval`: Refresh interval for watch mode in seconds (default: 10)
- `--recovery-type`: Filter by recovery type - PEER, DISK, or all (default: all)
- `--include-transitioning`: Include recently completed recoveries (DONE stage)

**Examples:**
```bash
# Check current recovery status
xmover monitor-recovery

# Monitor specific table recoveries
xmover monitor-recovery --table PartioffD

# Continuous monitoring with custom refresh rate
xmover monitor-recovery --watch --refresh-interval 5

# Monitor only PEER recoveries on specific node
xmover monitor-recovery --node data-hot-1 --recovery-type PEER

# Include completed recoveries still transitioning
xmover monitor-recovery --watch --include-transitioning
```

**Recovery Types:**
- **PEER**: Copying shard data from another node (replication/relocation)
- **DISK**: Rebuilding shard from local data (after restart/disk issues)


### `active-shards`
Monitor the most active shards by tracking checkpoint progression over time.
This command helps identify which shards are receiving the most write activity
by measuring local checkpoint progression between two snapshots.

**Options:**
- `--count`: Number of most active shards to show (default: 10)
- `--interval`: Observation interval in seconds (default: 30)
- `--min-checkpoint-delta`: Minimum checkpoint progression between snapshots to show shard (default: 1000)
- `--table, -t`: Monitor specific table only
- `--node, -n`: Monitor specific node only
- `--watch, -w`: Continuously monitor (refresh every interval)
- `--exclude-system`: Exclude system tables (gc.*, information_schema.*, *_events, *_log)
- `--min-rate`: Minimum activity rate (changes/sec) to show
- `--show-replicas/--hide-replicas`: Show replica shards (default: True)

**How it works:**
1. **Takes snapshot of ALL started shards** (not just currently active ones)
2. **Waits for observation interval** (configurable, default: 30 seconds)
3. **Takes second snapshot** of all started shards
4. **Compares snapshots** to find shards with checkpoint progression ≥ threshold
5. **Shows ranked results** with activity trends and insights

**Enhanced output features:**
- **Checkpoint visibility**: Shows actual `local_checkpoint` values (CP Start → CP End → Delta)
- **Partition awareness**: Separate tracking for partitioned tables (different partition_ident values)
- **Activity trends**: 🔥 HOT (≥100/s), 📈 HIGH (≥50/s), 📊 MED (≥10/s), 📉 LOW (<10/s)
- **Smart insights**: Identifies concentration patterns and load distribution (non-watch mode)
- **Flexible filtering**: Exclude system tables, set minimum rates, hide replicas
- **Context information**: Total activity, average rates, observation period
- **Clean watch mode**: Streamlined output without legend/insights for continuous monitoring

This approach captures shards that become active during the observation period, providing a complete view of cluster write patterns and identifying hot spots. The enhanced filtering helps focus on business-critical activity patterns.

**Sample output (single run):**
```
🔥 Most Active Shards (3 shown, 30s observation period)
Total checkpoint activity: 190,314 changes, Average rate: 2,109.0/sec
   Rank | Schema.Table           | Shard | Partition      | Node       | Type | Checkpoint Δ | Rate/sec | Trend
   -----------------------------------------------------------------------------------------------------------
   1    | gc.scheduled_jobs_log  | 0     | -              | data-hot-8 | P    | 113,744      | 3,791.5  | 🔥 HOT
   2    | TURVO.events           | 0     | 04732dpl6osj8d | data-hot-0 | P    | 45,837       | 1,527.9  | 🔥 HOT
   3    | doc.user_actions       | 1     | 04732dpk70rj6d | data-hot-2 | P    | 30,733       | 1,024.4  | 🔥 HOT
Legend:
  • Checkpoint Δ: Write operations during observation period
  • Partition: partition_ident (truncated if >14 chars, '-' if none)
Insights:
  • 3 HOT shards (≥100 changes/sec) - consider load balancing
  • All active shards are PRIMARY - normal write pattern
```

**Sample output (watch mode - cleaner):**
```
30s interval | threshold: 1,000 | top 5
🔥 Most Active Shards (3 shown, 30s observation period)
Total checkpoint activity: 190,314 changes, Average rate: 2,109.0/sec
   Rank | Schema.Table           | Shard | Partition      | Node       | Type | Checkpoint Δ | Rate/sec | Trend
   -----------------------------------------------------------------------------------------------------------
   1    | gc.scheduled_jobs_log  | 0     | -              | data-hot-8 | P    | 113,744      | 3,791.5  | 🔥 HOT
   2    | TURVO.events           | 0     | 04732dpl6osj8d | data-hot-0 | P    | 45,837       | 1,527.9  | 🔥 HOT
   3    | doc.user_actions       | 1     | 04732dpk70rj6d | data-hot-2 | P    | 30,733       | 1,024.4  | 🔥 HOT
━━━ Next update in 30s ━━━
```

#### Examples
```bash
# Show top 10 most active shards over 30 seconds
xmover active-shards

# Top 20 shards with 60-second observation period
xmover active-shards --count 20 --interval 60

# Continuous monitoring with 30-second intervals
xmover active-shards --watch --interval 30

# Monitor specific table activity
xmover active-shards --table my_table --watch

# Monitor specific node with custom threshold
xmover active-shards --node data-hot-1 --min-checkpoint-delta 500

# Exclude system tables and event logs for business data focus
xmover active-shards --exclude-system --count 20

# Only show high-activity shards (≥50 changes/sec)
xmover active-shards --min-rate 50 --count 15

# Focus on primary shards only
xmover active-shards --hide-replicas --count 20
```

#### Monitoring Active Shards and Write Patterns

Identify which shards are receiving the most write activity:

1. Quick snapshot of most active shards:
```bash
# Show top 10 most active shards over 30 seconds
xmover active-shards

# Longer observation period for more accurate results
xmover active-shards --count 15 --interval 60
```

2. Continuous monitoring for real-time insights:
```bash
# Continuous monitoring with 30-second intervals
xmover active-shards --watch --interval 30

# Monitor specific table for focused analysis
xmover active-shards --table critical_table --watch
```

3. Integration with rebalancing workflow:
```bash
# Identify hot shards first
xmover active-shards --count 20 --interval 60

# Move hot shards away from overloaded nodes
xmover recommend --table hot_table --prioritize-space --execute

# Monitor the impact
xmover active-shards --table hot_table --watch
```

### `test-connection`
Tests the connection to CrateDB and displays basic cluster information.

## Operation Modes

### Analysis vs Operational Views

XMover provides two distinct views of your cluster:

1. **Analysis View** (`analyze`, `zone-analysis`): Includes ALL shards regardless of state for complete cluster visibility
2. **Operational View** (`find-candidates`, `recommend`): Only includes healthy shards (STARTED + 100% recovered) for safe operations

### Prioritization Modes

When generating recommendations, you can choose between two prioritization strategies:

1. **Zone Balancing Priority** (default): Focuses on achieving optimal zone distribution first, then considers available space
2. **Space Priority**: Prioritizes moving shards to nodes with more available space, regardless of zone balance

### Safety Features

- **Zone Conflict Detection**: Prevents moves that would place multiple copies of the same shard in the same zone
- **Capacity Validation**: Ensures target nodes have sufficient free space
- **Health Checks**: Only operates on healthy shards (STARTED routing state + 100% recovery)
- **SQL Quoting**: Properly quotes schema and table names in generated SQL commands

## Example Workflows

### Regular Cluster Maintenance

1. Analyze current state:
```bash
xmover analyze
```

2. Check for zone imbalances:
```bash
xmover check-balance
```

3. Generate and review recommendations:
```bash
xmover recommend --dry-run
```

4. Execute safe moves:
```bash
xmover recommend --execute
```

### Targeted Node Relief

When a specific node is running low on space:

1. Check which node needs relief:
```bash
xmover analyze
```

2. Generate recommendations for that specific node:
```bash
xmover recommend --prioritize-space --node data-hot-4 --dry-run
```

3. Execute the moves:
```bash
xmover recommend --prioritize-space --node data-hot-4 --execute
```

### Monitoring Shard Recovery Operations

After executing shard moves, monitor the recovery progress:

1. Execute moves and monitor recovery:
```bash
# Execute moves
xmover recommend --node data-hot-1 --execute

# Monitor the resulting recoveries
xmover monitor-recovery --watch
```

2. Monitor specific table or node recovery:
```bash
# Monitor specific table
xmover monitor-recovery --table shipmentFormFieldData --watch

# Monitor specific node
xmover monitor-recovery --node data-hot-4 --watch

# Monitor including completed recoveries
xmover monitor-recovery --watch --include-transitioning
```

3. Check recovery after node maintenance:
```bash
# After bringing a node back online
xmover monitor-recovery --node data-hot-3 --recovery-type DISK
```

### Manual Shard Movement

1. Validate the move first:
```bash
xmover validate-move SCHEMA.TABLE SHARD_ID FROM_NODE TO_NODE
```

2. Generate safe recommendations:
```bash
xmover recommend --prioritize-space --execute
```

3. Monitor shard health after moves

### Troubleshooting Zone Conflicts

1. Identify conflicts:
```bash
xmover zone-analysis --show-shards
```

2. Generate targeted fixes:
```bash
xmover recommend --prioritize-zones --execute
```

## Configuration

### Environment Variables

- `CRATE_CONNECTION_STRING`: CrateDB HTTP endpoint (required)
- `CRATE_USERNAME`: Username for authentication (optional)
- `CRATE_PASSWORD`: Password for authentication (optional)
- `CRATE_SSL_VERIFY`: Enable SSL certificate verification (default: true)

### Connection String Format

```text
https://hostname:port
```

The tool automatically appends `/_sql` to the endpoint.

## Safety Considerations

⚠️ **Important Safety Notes:**

1. **Always test in non-production environments first**
2. **Monitor shard health after each move before proceeding with additional moves**
3. **Ensure adequate cluster capacity before decommissioning nodes**
4. **Verify zone distribution after rebalancing operations**
5. **Keep backups current before performing large-scale moves**

## Troubleshooting

XMover provides comprehensive troubleshooting tools to help diagnose and resolve shard movement issues.

### Quick Diagnosis Commands

```bash
# Validate a specific move before execution
xmover validate-move SCHEMA.TABLE SHARD_ID FROM_NODE TO_NODE

# Explain CrateDB error messages
xmover explain-error "your error message here"

# Check zone distribution for conflicts
xmover zone-analysis --show-shards

# Verify overall cluster health
xmover analyze
```

### Common Issues and Solutions

1. **Zone Conflicts**
   ```text
   Error: "NO(a copy of this shard is already allocated to this node)"
   ```
   - **Cause**: Target node already has a copy of the shard
   - **Solution**: Use `xmover zone-analysis --show-shards` to find alternative targets
   - **Prevention**: Always use `xmover validate-move` before executing moves

2. **Zone Allocation Limits**
   ```text
   Error: "too many copies of the shard allocated to nodes with attribute [zone]"
   ```
   - **Cause**: CrateDB's zone awareness prevents too many copies in same zone
   - **Solution**: Move shard to a different availability zone
   - **Prevention**: Use `xmover recommend` which respects zone constraints

3. **Insufficient Space**
   ```text
   Error: "not enough disk space"
   ```
   - **Cause**: Target node lacks sufficient free space
   - **Solution**: Choose node with more capacity or free up space
   - **Check**: `xmover analyze` to see available space per node

4. **High Disk Usage Blocking Moves**
   ```text
   Error: "Target node disk usage too high (85.3%)"
   ```
   - **Cause**: Target node exceeds default 85% disk usage threshold
   - **Solution**: Use `--max-disk-usage` to allow higher usage for urgent moves
   - **Example**: `xmover recommend --max-disk-usage 90 --prioritize-space`

5. **No Recommendations Generated**
   - **Cause**: Cluster may already be well balanced
   - **Solution**: Adjust size filters or check `xmover check-balance`
   - **Try**: `--prioritize-space` mode for capacity-based moves

### Error Message Decoder

Use the built-in error decoder for complex CrateDB messages:

```bash
# Interactive mode - paste your error message
xmover explain-error

# Direct analysis
xmover explain-error "NO(a copy of this shard is already allocated to this node)"
```

### Configurable Safety Thresholds

XMover uses configurable safety thresholds to prevent risky moves:

**Disk Usage Threshold (default: 85%)**
```bash
# Allow moves to nodes with higher disk usage
xmover recommend --max-disk-usage 90 --prioritize-space

# For urgent space relief
xmover validate-move <SCHEMA.TABLE> <SHARD_ID> <FROM> <TO> --max-disk-usage 95
```

**When to Adjust Thresholds:**
- **Emergency situations**: Increase to 90-95% for critical space relief
- **Conservative operations**: Decrease to 75-80% for safer moves
- **Staging environments**: Can be more aggressive (90%+)
- **Production**: Keep conservative (80-85%)

### Advanced Troubleshooting

For detailed troubleshooting procedures, see {ref}`xmover-troubleshooting` which covers:
- Step-by-step diagnostic procedures
- Emergency recovery procedures
- Best practices for safe operations
- Complete error reference guide

### Debug Information

All commands provide detailed safety validation messages and explanations for any issues detected.
