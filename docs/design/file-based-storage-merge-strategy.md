# Git Merge Strategy for File-Based Storage

## Overview

This document describes the git merge strategy for GraphlessDB's file-based storage. The design ensures that concurrent modifications can be merged safely while maintaining data consistency.

## Merge Strategy Goals

1. **Automatic merging** when possible
2. **Safe conflict detection** for incompatible changes
3. **Clear conflict resolution** guidance
4. **Data consistency** guarantees after merge

## Git Configuration

### .gitattributes

The repository should include a `.gitattributes` file at the storage root:

```gitattributes
# File-based storage merge configuration
*.jsonl text eol=lf merge=union
.metadata/** -diff -merge
.locks/** -diff -merge
```

### Explanation

- `*.jsonl text eol=lf`: All JSONL files use Unix line endings (LF)
- `merge=union`: Combine both sides of changes (additive merge)
- `.metadata/** -diff -merge`: Don't track or merge metadata files
- `.locks/** -diff -merge`: Don't track lock files

## Union Merge Strategy

### How It Works

The `union` merge driver:
1. Takes all unique lines from both branches
2. Sorts them (maintaining file order)
3. Produces a merged file with all changes

### Why It Works for JSONL

Since each triple is on a separate line:
- Adding triples = adding lines (no conflict)
- Different subjects/predicates = different lines (merge cleanly)
- Same subject but different triples = different lines (merge cleanly)

### When Conflicts Occur

True conflicts happen when:
- Same triple (same line) modified differently
- Delete on one branch, modify on another
- Non-commutative changes

## Merge Scenarios

### Scenario 1: Independent Additions

**Scenario**: Two users add different triples

**Branch A**:
```json
{"subject":"user:124","predicate":"name","indexedObject":"alice","object":"Alice","partition":"0","versionDetail":null}
```

**Branch B**:
```json
{"subject":"user:125","predicate":"name","indexedObject":"bob","object":"Bob","partition":"0","versionDetail":null}
```

**Merge Result**:
```json
{"subject":"user:124","predicate":"name","indexedObject":"alice","object":"Alice","partition":"0","versionDetail":null}
{"subject":"user:125","predicate":"name","indexedObject":"bob","object":"Bob","partition":"0","versionDetail":null}
```

**Status**: ✅ Auto-merged successfully

### Scenario 2: Different Properties of Same Subject

**Scenario**: Two users modify different properties of the same subject

**Base**:
```json
{"subject":"user:123","predicate":"name","indexedObject":"john","object":"John","partition":"0","versionDetail":null}
```

**Branch A** adds email:
```json
{"subject":"user:123","predicate":"name","indexedObject":"john","object":"John","partition":"0","versionDetail":null}
{"subject":"user:123","predicate":"email","indexedObject":"john@example.com","object":"john@example.com","partition":"0","versionDetail":null}
```

**Branch B** adds age:
```json
{"subject":"user:123","predicate":"name","indexedObject":"john","object":"John","partition":"0","versionDetail":null}
{"subject":"user:123","predicate":"age","indexedObject":"30","object":"30","partition":"0","versionDetail":null}
```

**Merge Result**:
```json
{"subject":"user:123","predicate":"name","indexedObject":"john","object":"John","partition":"0","versionDetail":null}
{"subject":"user:123","predicate":"age","indexedObject":"30","object":"30","partition":"0","versionDetail":null}
{"subject":"user:123","predicate":"email","indexedObject":"john@example.com","object":"john@example.com","partition":"0","versionDetail":null}
```

**Status**: ✅ Auto-merged successfully

### Scenario 3: Same Property Modified Differently (CONFLICT)

**Scenario**: Two users modify the same property of the same subject

**Base**:
```json
{"subject":"user:123","predicate":"email","indexedObject":"old@example.com","object":"old@example.com","partition":"0","versionDetail":{"version":1,"timestamp":"2024-01-15T10:00:00Z"}}
```

**Branch A**:
```json
{"subject":"user:123","predicate":"email","indexedObject":"alice@example.com","object":"alice@example.com","partition":"0","versionDetail":{"version":2,"timestamp":"2024-01-15T11:00:00Z"}}
```

**Branch B**:
```json
{"subject":"user:123","predicate":"email","indexedObject":"bob@example.com","object":"bob@example.com","partition":"0","versionDetail":{"version":2,"timestamp":"2024-01-15T11:30:00Z"}}
```

**Git Conflict**:
```
<<<<<<< HEAD
{"subject":"user:123","predicate":"email","indexedObject":"alice@example.com","object":"alice@example.com","partition":"0","versionDetail":{"version":2,"timestamp":"2024-01-15T11:00:00Z"}}
=======
{"subject":"user:123","predicate":"email","indexedObject":"bob@example.com","object":"bob@example.com","partition":"0","versionDetail":{"version":2,"timestamp":"2024-01-15T11:30:00Z"}}
>>>>>>> branch-b
```

**Resolution Strategy**: Choose the triple with the later timestamp (Branch B)

**Resolved**:
```json
{"subject":"user:123","predicate":"email","indexedObject":"bob@example.com","object":"bob@example.com","partition":"0","versionDetail":{"version":2,"timestamp":"2024-01-15T11:30:00Z"}}
```

**Status**: ⚠️ Manual resolution required

### Scenario 4: Delete vs Modify (CONFLICT)

**Scenario**: One branch deletes a triple, another modifies it

**Base**:
```json
{"subject":"user:123","predicate":"status","indexedObject":"active","object":"active","partition":"0","versionDetail":null}
```

**Branch A** (deletes):
```json
(line removed)
```

**Branch B** (modifies):
```json
{"subject":"user:123","predicate":"status","indexedObject":"inactive","object":"inactive","partition":"0","versionDetail":null}
```

**Git Conflict**:
```
<<<<<<< HEAD
=======
{"subject":"user:123","predicate":"status","indexedObject":"inactive","object":"inactive","partition":"0","versionDetail":null}
>>>>>>> branch-b
```

**Resolution Strategy**: 
- **Business rule dependent**
- Default: Delete wins (keep empty)
- Alternative: Modification wins if critical update

**Status**: ⚠️ Manual resolution required

### Scenario 5: Different Partitions

**Scenario**: Changes to different partitions

**Branch A** modifies `partition-0.jsonl`

**Branch B** modifies `partition-1.jsonl`

**Merge Result**: Both files updated independently

**Status**: ✅ Auto-merged successfully (different files)

### Scenario 6: Index Consistency

**Scenario**: Data file updated, indexes need rebuilding

**Strategy**: 
- After merge, automatically rebuild indexes
- Run validation to ensure index consistency
- Log any discrepancies

## Conflict Resolution Guidelines

### Timestamp-Based Resolution

**Rule**: When two modifications conflict, choose the one with the later timestamp

**Implementation**:
1. Parse both conflicting JSON lines
2. Compare `versionDetail.timestamp` values
3. Keep the triple with the later timestamp
4. If timestamps equal, use business-specific tie-breaker

### Last-Write-Wins (LWW)

For concurrent modifications without timestamps:
1. Use git commit timestamp as proxy
2. Compare commit times
3. Keep changes from later commit

### Manual Resolution Process

1. **Identify conflict type**
   - Property modification
   - Delete vs modify
   - Version mismatch

2. **Apply resolution rule**
   - Timestamp comparison
   - Business logic
   - User decision

3. **Validate result**
   - Ensure valid JSON
   - Check schema compliance
   - Verify referential integrity

4. **Update indexes**
   - Rebuild affected indexes
   - Validate consistency

## Merge Validation

After any merge (automatic or manual), run validation:

### 1. JSON Validity
```bash
for file in data/*.jsonl; do
  jq empty "$file" || echo "Invalid JSON in $file"
done
```

### 2. Schema Compliance
```bash
# Ensure all required fields present
jq -c 'select(.subject == null or .predicate == null)' data/*.jsonl
```

### 3. Index Consistency
```bash
# Rebuild and compare indexes
./scripts/rebuild-indexes.sh
./scripts/validate-indexes.sh
```

### 4. Partition Integrity
```bash
# Verify each triple is in correct partition
./scripts/validate-partitions.sh
```

## Merge Helper Tool

A CLI tool (`graphlessdb-merge-tool`) should be provided:

### Features

1. **Detect conflicts**
   ```bash
   graphlessdb-merge-tool detect --storage-path /path/to/storage
   ```

2. **Auto-resolve by timestamp**
   ```bash
   graphlessdb-merge-tool resolve --strategy timestamp --storage-path /path/to/storage
   ```

3. **Validate merge result**
   ```bash
   graphlessdb-merge-tool validate --storage-path /path/to/storage
   ```

4. **Rebuild indexes**
   ```bash
   graphlessdb-merge-tool rebuild-indexes --storage-path /path/to/storage
   ```

### Example Usage

```bash
# After git merge with conflicts
git merge feature-branch
# CONFLICT in data/partition-0.jsonl

# Use merge tool to resolve
graphlessdb-merge-tool resolve --strategy timestamp --storage-path ./storage

# Validate result
graphlessdb-merge-tool validate --storage-path ./storage

# Rebuild indexes
graphlessdb-merge-tool rebuild-indexes --storage-path ./storage

# Complete merge
git add storage/
git commit -m "Merge feature-branch with auto-resolved conflicts"
```

## Best Practices

### 1. Small, Frequent Commits

- Commit often to minimize merge complexity
- Keep changes focused and atomic
- Easier to identify conflict source

### 2. Pull Before Push

- Always pull latest changes before pushing
- Resolve conflicts locally first
- Test merged result before pushing

### 3. Partition Strategy

- Assign related data to same partition
- Minimize cross-partition changes
- Different teams work on different partitions

### 4. Testing After Merge

- Run full test suite after merge
- Verify data integrity
- Check index consistency

### 5. Communication

- Coordinate with team on large changes
- Document manual conflict resolutions
- Share merge outcomes

## Advanced Scenarios

### Multi-Way Merges

When merging multiple branches:
1. Merge branches sequentially
2. Validate after each merge
3. Rebuild indexes at the end

### Rebase Workflow

Alternative to merge:
1. Rebase feature branch on main
2. Resolve conflicts during rebase
3. Force-push updated branch
4. Fast-forward merge to main

### Cherry-Pick

For selective merging:
1. Cherry-pick specific commits
2. Resolve conflicts as usual
3. Validate and rebuild indexes

## Rollback Strategy

If merge goes wrong:

### 1. Abort Merge
```bash
git merge --abort
```

### 2. Revert Merge Commit
```bash
git revert -m 1 <merge-commit-hash>
```

### 3. Restore from Backup
```bash
cp -r storage.backup/ storage/
```

## Monitoring and Alerts

Track merge metrics:
- Conflict frequency
- Resolution time
- Automatic vs manual resolutions
- Post-merge errors

Alert on:
- High conflict rate
- Failed validations
- Index inconsistencies
- Long-running conflict resolution

## Future Enhancements

1. **Custom merge driver**: Implement git merge driver for automatic timestamp resolution
2. **Conflict prevention**: Pre-merge validation and warnings
3. **Visual merge tool**: GUI for conflict resolution
4. **Automated testing**: Post-merge integration tests

## References

- Git Merge Documentation: https://git-scm.com/docs/git-merge
- Git Attributes: https://git-scm.com/docs/gitattributes
- JSON Lines Format: https://jsonlines.org/
- File Format Design: file-based-storage-format.md
- Storage Structure Design: file-based-storage-structure.md
