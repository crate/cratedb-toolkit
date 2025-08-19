(xmover-troubleshooting)=
# Troubleshooting CrateDB using XMover

This guide helps you diagnose and resolve common issues when using XMover for CrateDB shard management.

## Quick Diagnosis Commands

Before troubleshooting, run these commands to understand your cluster state:

```bash
# Check overall cluster health
xmover analyze

# Check zone distribution for conflicts
xmover zone-analysis --show-shards

# Validate a specific move before execution
xmover validate-move SCHEMA.TABLE SHARD_ID FROM_NODE TO_NODE

# Explain CrateDB error messages
xmover explain-error "your error message here"
```

## Common Issues and Solutions

### 1. Zone Conflicts

#### Symptoms
- Error: `NO(a copy of this shard is already allocated to this node)`
- Error: `NO(there are too many copies of the shard allocated to nodes with attribute [zone])`
- Recommendations show zone conflicts in safety validation

#### Root Causes
- Target node already has a copy of the shard (primary or replica)
- Target zone already has copies, violating CrateDB's zone awareness
- Incorrect understanding of current shard distribution

#### Solutions

**Step 1: Analyze Current Distribution**
```bash
# See exactly where shard copies are located
xmover zone-analysis --show-shards --table YOUR_TABLE

# Check overall zone balance
xmover check-balance
```

**Step 2: Find Alternative Targets**
```bash
# Find nodes with available capacity in different zones
xmover analyze

# Get movement candidates with size filters
xmover find-candidates --min-size 20 --max-size 30
```

**Step 3: Validate Before Moving**
```bash
# Always validate moves before execution
xmover validate-move SCHEMA.TABLE SHARD_ID FROM_NODE TO_NODE
```

#### Prevention
- Always use `xmover recommend` instead of manual moves
- Enable dry-run mode by default: `xmover recommend --dry-run`
- Check zone distribution before planning moves

### 2. Insufficient Space Issues

#### Symptoms
- Error: `not enough disk space`
- Safety validation fails with space warnings
- High disk usage percentages in cluster analysis

#### Root Causes
- Target node doesn't have enough free space for the shard
- High disk usage on target nodes (>85%)
- Insufficient buffer space for safe operations

#### Solutions

**Step 1: Check Available Space**
```bash
# Review node capacity and usage
xmover analyze

# Look for nodes with more available space
xmover find-candidates --min-size 0 --max-size 100
```

**Step 2: Adjust Parameters**
```bash
# Increase minimum free space requirement
xmover recommend --min-free-space 200

# Focus on smaller shards
xmover recommend --max-size 50
```

**Step 3: Free Up Space**
- Delete old snapshots and unused data
- Move other shards away from constrained nodes
- Consider adding nodes to the cluster

#### Prevention
- Monitor disk usage regularly with `xmover analyze`
- Set conservative `--min-free-space` values (default: 100GB)
- Plan capacity expansion before reaching 80% disk usage

### 3. Node Performance Issues

#### Symptoms
- Error: `shard recovery limit`
- High heap usage warnings
- Slow shard movement operations

#### Root Causes
- Too many concurrent shard movements
- High heap usage on target nodes (>80%)
- Resource contention during moves

#### Solutions

**Step 1: Check Node Health**
```bash
# Review heap and disk usage
xmover analyze

# Check for overloaded nodes
xmover check-balance
```

**Step 2: Reduce Concurrent Operations**
```bash
# Move fewer shards at once
xmover recommend --max-moves 3

# Wait between moves for recovery completion
# Monitor with CrateDB Admin UI
```

**Step 3: Target Less Loaded Nodes**
```bash
# Prioritize nodes with better resources
xmover recommend --prioritize-space
```

#### Prevention
- Move shards gradually (5-10 at a time)
- Monitor heap usage and wait for recovery completion
- Avoid moves during high-traffic periods

### 4. Zone Imbalance Issues

#### Symptoms
- `check-balance` shows zones marked as "Over" or "Under"
- Zone distribution is uneven
- Some zones have significantly more shards

#### Root Causes
- Historical data distribution patterns
- Node additions/removals without rebalancing
- Tables created with poor initial distribution

#### Solutions

**Step 1: Assess Imbalance**
```bash
# Check current zone balance
xmover check-balance --tolerance 15

# Get detailed zone analysis
xmover zone-analysis
```

**Step 2: Generate Rebalancing Plan**
```bash
# Prioritize zone balancing
xmover recommend --prioritize-zones --dry-run

# Review recommendations carefully
xmover recommend --prioritize-zones --max-moves 10
```

**Step 3: Execute Gradually**
```bash
# Execute in small batches
xmover recommend --prioritize-zones --max-moves 5 --execute

# Monitor progress and repeat
```

#### Prevention
- Run regular balance checks: `xmover check-balance`
- Use zone-aware table creation with proper shard allocation
- Plan rebalancing during maintenance windows

### 5. Connection and Authentication Issues

#### Symptoms
- "Connection failed" errors
- Authentication failures
- SSL/TLS errors

#### Root Causes
- Incorrect connection string in `.env`
- Wrong credentials
- Network connectivity issues
- SSL certificate problems

#### Solutions

**Step 1: Verify Connection**
```bash
# Test basic connectivity
xmover test-connection
```

**Step 2: Check Configuration**
```bash
# Verify .env file contents
cat .env

# Example correct format:
CRATE_CONNECTION_STRING=https://cluster.cratedb.net:4200
CRATE_USERNAME=admin
CRATE_PASSWORD=your-password
CRATE_SSL_VERIFY=true
```

**Step 3: Test Network Access**
```bash
# Test HTTP connectivity
curl -u username:password https://your-cluster:4200/_sql -d '{"stmt":"SELECT 1"}'
```

#### Prevention
- Use `.env.example` as a template
- Verify credentials with CrateDB admin
- Test connectivity from deployment environment

## Error Message Decoder

### CrateDB Allocation Errors

Use `xmover explain-error` to decode complex CrateDB error messages:

```bash
# Interactive mode
xmover explain-error

# Direct analysis
xmover explain-error "your error message here"
```

### Common Error Patterns

| Error Pattern | Meaning | Quick Fix |
|---------------|---------|-----------|
| `copy of this shard is already allocated` | Node already has shard | Choose different target node |
| `too many copies...with attribute [zone]` | Zone limit exceeded | Move to different zone |
| `not enough disk space` | Insufficient space | Free space or choose different node |
| `shard recovery limit` | Too many concurrent moves | Wait and retry with fewer moves |
| `allocation is disabled` | Cluster allocation disabled | Re-enable allocation settings |

## Best Practices for Safe Operations

### Pre-Move Checklist

1. **Analyze cluster state**
   ```bash
   xmover analyze
   ```

2. **Check zone distribution**
   ```bash
   xmover zone-analysis
   ```

3. **Generate recommendations**
   ```bash
   xmover recommend --dry-run
   ```

4. **Validate specific moves**
   ```bash
   xmover validate-move SCHEMA.TABLE SHARD_ID FROM TO
   ```

5. **Execute gradually**
   ```bash
   xmover recommend --max-moves 5 --execute
   ```

### During Operations

1. **Monitor shard health**
   - Check CrateDB Admin UI for recovery progress
   - Watch for failed or stuck shards
   - Verify routing state changes to STARTED

2. **Track resource usage**
   - Monitor disk and heap usage on target nodes
   - Watch for network saturation during moves
   - Check cluster performance metrics

3. **Maintain documentation**
   - Record moves performed and reasons
   - Note any issues encountered
   - Document lessons learned

### Post-Move Verification

1. **Verify shard health**
   ```sql
   SELECT table_name, id, "primary", node['name'], routing_state 
   FROM sys.shards 
   WHERE table_name = 'your_table' AND routing_state != 'STARTED';
   ```

2. **Check zone balance**
   ```bash
   xmover check-balance
   ```

3. **Monitor cluster performance**
   - Query response times
   - Resource utilization
   - Error rates

## Emergency Procedures

### Stuck Shard Recovery

If a shard gets stuck during movement:

1. **Check shard status**
   ```sql
   SELECT * FROM sys.shards WHERE routing_state != 'STARTED';
   ```

2. **Cancel problematic moves**
   ```sql
   ALTER TABLE "schema"."table" REROUTE CANCEL SHARD <shard_id> ON '<node_name>';
   ```

3. **Retry allocation**
   ```sql
   ALTER TABLE "schema"."table" REROUTE RETRY FAILED;
   ```

### Cluster Health Issues

If moves cause cluster problems:

1. **Disable allocation temporarily**
   ```text
   PUT /_cluster/settings
   {
     "persistent": {
       "cluster.routing.allocation.enable": "primaries"
     }
   }
   ```

2. **Wait for stabilization**
   - Monitor cluster health
   - Check node resource usage
   - Verify no failed shards

3. **Re-enable allocation**
   ```text
   PUT /_cluster/settings
   {
     "persistent": {
       "cluster.routing.allocation.enable": "all"
     }
   }
   ```

## Getting Help

### Built-in Help

```bash
# Command help
xmover --help
xmover COMMAND --help

# Error explanation
xmover explain-error

# Move validation
xmover validate-move SCHEMA.TABLE SHARD_ID FROM TO
```

### Additional Resources

- **CrateDB Documentation**: https://crate.io/docs/
- **Shard Allocation Guide**: https://crate.io/docs/crate/reference/en/latest/admin/system-information.html
- **Cluster Settings**: https://crate.io/docs/crate/reference/en/latest/config/cluster.html

### Reporting Issues

When reporting issues, include:

1. **XMover version and command used**
2. **Complete error message**
3. **Cluster information** (`xmover analyze` output)
4. **Zone analysis** (`xmover zone-analysis` output)
5. **CrateDB version and configuration**

### Support Checklist

Before contacting support:

- [ ] Tried `xmover validate-move` for the specific operation
- [ ] Checked zone distribution with `xmover zone-analysis`
- [ ] Reviewed cluster health with `xmover analyze`
- [ ] Used `xmover explain-error` to decode error messages
- [ ] Verified connection and authentication with `xmover test-connection`
- [ ] Read through this troubleshooting guide
- [ ] Checked CrateDB documentation for allocation settings
