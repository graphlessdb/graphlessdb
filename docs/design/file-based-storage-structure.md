# Storage Structure Design for File-Based Storage

## Overview

This document describes the directory and file organization for GraphlessDB's file-based storage implementation. The design follows the architecture of `InMemoryRDFTripleStore` while optimizing for file system operations and git merge workflows.

## Directory Structure

```
{root_storage_path}/
├── data/
│   ├── partition-0.jsonl
│   ├── partition-1.jsonl
│   ├── partition-2.jsonl
│   └── partition-N.jsonl
├── indexes/
│   ├── by-predicate/
│   │   ├── partition-0.jsonl
│   │   ├── partition-1.jsonl
│   │   └── partition-N.jsonl
│   └── by-indexed-object/
│       ├── partition-0.jsonl
│       ├── partition-1.jsonl
│       └── partition-N.jsonl
├── .metadata/
│   ├── config.json
│   └── statistics.json
└── .gitattributes
```

## Directory Descriptions

### `/data` - Primary Triple Storage

Contains the main RDF triple data partitioned into separate files.

**File naming**: `partition-{N}.jsonl` where N is the partition number (0-based)

**Content**: Each file contains all triples for that partition in JSONL format (see file-based-storage-format.md)

**Partitioning strategy**:
- Number of partitions is configured via `GraphOptions.PartitionCount`
- Partition assignment follows the same algorithm as `InMemoryRDFTripleStore`
- Default partition count: 10 (configurable)

### `/indexes` - Secondary Indexes

Contains secondary indexes to support efficient query operations.

#### `/indexes/by-predicate`

Supports queries by predicate (e.g., "find all triples with predicate 'email'")

**File naming**: `partition-{N}.jsonl`

**Content**: Index entries in JSONL format:
```json
{
  "predicate": "string",
  "subject": "string",
  "indexedObject": "string",
  "partition": "string"
}
```

**Sort order**: `partition → predicate → subject → indexedObject`

#### `/indexes/by-indexed-object`

Supports queries by indexed object (e.g., "find all triples with object 'john@example.com'")

**File naming**: `partition-{N}.jsonl`

**Content**: Index entries in JSONL format:
```json
{
  "indexedObject": "string",
  "subject": "string",
  "predicate": "string",
  "partition": "string"
}
```

**Sort order**: `partition → indexedObject → subject → predicate`

### `/.metadata` - Storage Metadata

Contains configuration and runtime metadata. This directory should be excluded from git tracking.

#### `config.json`

Storage configuration:
```json
{
  "tableName": "string",
  "partitionCount": "number",
  "formatVersion": "1.0",
  "created": "ISO8601-timestamp",
  "lastModified": "ISO8601-timestamp"
}
```

#### `statistics.json`

Runtime statistics (updated periodically):
```json
{
  "totalTriples": "number",
  "triplesByPartition": {
    "0": "number",
    "1": "number"
  },
  "lastCompaction": "ISO8601-timestamp",
  "fileSize": {
    "data": "bytes",
    "indexes": "bytes"
  }
}
```

### `.gitattributes`

Git configuration for merge strategies:
```
*.jsonl text eol=lf merge=union
.metadata/** -diff -merge
```

This ensures:
- Consistent line endings across platforms
- Union merge strategy for JSONL files (combine both sides)
- Metadata files are not tracked in git

## File Organization Principles

### 1. Partition Isolation

Each partition has its own file to:
- Enable parallel reads/writes
- Minimize merge conflicts (different partitions = different files)
- Support concurrent access patterns

### 2. Index Separation

Indexes are in separate files from data to:
- Allow independent updates
- Reduce file sizes for better git performance
- Enable index rebuild without touching data files

### 3. Sorted Content

Files are kept sorted to:
- Enable binary search for lookups
- Maximize git merge success
- Make manual inspection easier

### 4. Metadata Isolation

Metadata is separate to:
- Prevent merge conflicts (not tracked in git)
- Store frequently-changing statistics
- Maintain configuration independently

## File Lifecycle Operations

### Initialization

1. Create directory structure
2. Create `config.json` with initial settings
3. Create empty partition files (0 to N-1)
4. Create empty index partition files
5. Initialize `statistics.json`

### Write Operations

**Adding a triple**:
1. Determine partition number from subject
2. Append triple to `data/partition-{N}.jsonl`
3. Append index entries to both index files for partition N
4. Update statistics (periodically, not on every write)

**Updating a triple**:
1. Load partition file into memory
2. Update or replace the specific triple (by matching key)
3. Re-sort the partition
4. Write entire partition back to file
5. Update index files similarly

**Deleting a triple**:
1. Load partition file into memory
2. Remove the specific triple
3. Write partition back to file
4. Remove index entries

### Read Operations

**Get by key**:
1. Determine partition from key
2. Read and parse `data/partition-{N}.jsonl`
3. Find matching triple(s)

**Query by predicate**:
1. Determine partition (if specified)
2. Read `indexes/by-predicate/partition-{N}.jsonl`
3. Find matching entries
4. Retrieve full triples from data files

**Scan**:
1. Read all partition files sequentially
2. Return triples up to limit
3. Track position for pagination

### Compaction

Periodically compact files to:
- Remove deleted triples
- Ensure consistent sorting
- Rebuild indexes from data

**Process**:
1. For each partition:
   - Read all triples
   - Filter out deleted/superseded triples
   - Re-sort
   - Write to new file
   - Atomically replace old file
2. Rebuild all indexes
3. Update statistics

## Concurrency and Locking

### File Locking Strategy

**Read operations**:
- Use shared file locks
- Multiple readers allowed
- No lock needed for read-only scenarios

**Write operations**:
- Use exclusive file locks per partition
- Lock specific partition file before writing
- Other partitions remain accessible

**Lock implementation**:
- Use `FileStream` with `FileShare.Read` for shared locks
- Use `FileShare.None` for exclusive locks
- Implement retry logic with exponential backoff

### Multi-Process Access

**Same process**:
- Use in-memory locks (`Lock` class)
- Track partition-level locks

**Different processes**:
- Use file system locks
- Advisory locks on lock files: `.locks/partition-{N}.lock`
- Clean up stale locks on startup

## Performance Considerations

### Caching Strategy

1. **In-Memory Cache**: Cache frequently accessed partitions
2. **Write Buffer**: Buffer writes and flush periodically
3. **Index Cache**: Keep index lookups in memory

### File Size Management

- **Split threshold**: Split partition files larger than 100MB
- **Merge threshold**: Merge partition files smaller than 1MB
- **Compaction schedule**: Daily or after significant deletes

### Optimization Techniques

1. **Memory-mapped files**: For read-heavy workloads
2. **Streaming reads**: For large scans
3. **Batch writes**: Group multiple writes into single file operation
4. **Lazy loading**: Load partitions on-demand

## Git Merge Strategy

### Automatic Merge Scenarios

Files are designed to merge automatically when:
- Different partitions are modified (different files)
- Different subjects within same partition (different lines)
- Additions only (append to end)

### Conflict Resolution

When conflicts occur:
1. Git marks conflicts at line level
2. Each conflicting triple is on a separate line
3. Resolution strategy:
   - Choose triple with later timestamp
   - Or keep both if different predicates
   - Or apply business-specific rules

### Merge Helper Tool

A CLI tool will be provided to:
- Detect conflicts in JSONL files
- Auto-resolve based on timestamp
- Validate merged results
- Rebuild indexes after merge

## Migration and Compatibility

### From InMemoryRDFTripleStore

**Export process**:
1. Iterate all partitions
2. Write each partition to corresponding file
3. Build index files from partition data
4. Create metadata files

**Import process**:
1. Read all partition files
2. Load into memory structures
3. Verify data integrity
4. Build in-memory indexes

### Data Versioning

**Format version** in `config.json` allows:
- Schema evolution
- Backward compatibility checks
- Migration between versions

## Error Handling

### File Corruption

- Validate JSON on read
- Skip invalid lines, log errors
- Maintain backup copies
- Rebuild from last known good state

### Missing Files

- Create missing partition files on-demand
- Log warnings for unexpected missing files
- Validate directory structure on startup

### Lock Failures

- Retry with exponential backoff
- Timeout after configurable period
- Log detailed error information
- Implement deadlock detection

## Security Considerations

### File Permissions

- Data directory: Read/write for application user only
- Metadata directory: Read/write for application user only
- Recommended: 0700 for directories, 0600 for files

### Path Traversal

- Validate all partition numbers
- Reject paths outside storage root
- Sanitize any user-provided path components

## Monitoring and Observability

### Metrics to Track

- File sizes per partition
- Read/write operation counts
- Lock contention frequency
- Compaction duration
- Index rebuild time

### Logging

- File operations (open, close, lock)
- Compaction events
- Merge conflicts detected
- Performance warnings

## Testing Strategy

### Unit Tests

- Directory creation
- File read/write operations
- Locking mechanisms
- Index maintenance

### Integration Tests

- Multi-partition operations
- Concurrent access
- File compaction
- Data integrity

### Git Merge Tests

- Simulated concurrent modifications
- Conflict detection
- Merge resolution
- Data consistency after merge

## References

- InMemoryRDFTripleStore implementation (src/GraphlessDB/Storage.Services.Internal.InMemory/)
- File Format Design (file-based-storage-format.md)
- Git Attributes Documentation: https://git-scm.com/docs/gitattributes
