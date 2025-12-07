# File Format Design for Git Merge Compatibility

## Overview

This document describes the file format design for GraphlessDB's file-based storage implementation. The primary goals are:
- **Git merge compatibility**: Enable automatic merging of concurrent changes
- **Conflict detection**: Make conflicts obvious and easy to resolve
- **Human readability**: Allow manual inspection and editing when needed
- **Performance**: Efficient read/write operations

## File Format Specification

### Format Choice: JSON Lines (JSONL)

We will use **JSON Lines** format (also known as newline-delimited JSON) for storing RDF triples. Each line contains a single JSON object representing one RDF triple.

#### Rationale

1. **Git Merge Friendly**: Each triple is on its own line, so:
   - Adding new triples appends new lines (no conflicts)
   - Modifying a triple only changes that specific line
   - Deleting a triple removes only that line
   - Line-based diff tools work naturally

2. **Conflict Resolution**: When conflicts occur:
   - Each conflicting triple is clearly visible as a separate line
   - Git conflict markers work at the triple level
   - Resolution involves choosing/combining specific triples

3. **Human Readable**: 
   - JSON is widely understood
   - Each line is self-contained
   - Easy to inspect with standard tools (grep, jq, etc.)

4. **Performance**:
   - Append-only writes are efficient
   - Can stream large files without loading everything into memory
   - Standard JSON parsers are optimized

### RDF Triple JSON Schema

Each line represents a single RDF triple with the following structure:

```json
{
  "subject": "string",
  "predicate": "string",
  "indexedObject": "string",
  "object": "string",
  "partition": "string",
  "versionDetail": {
    "version": "number",
    "timestamp": "ISO8601-string"
  } | null
}
```

#### Field Descriptions

- **subject**: The subject of the RDF triple (required)
- **predicate**: The predicate/property of the RDF triple (required)
- **indexedObject**: Indexed version of the object for efficient lookups (required)
- **object**: The actual object value (required)
- **partition**: Partition identifier for distributed storage (required)
- **versionDetail**: Optional versioning information
  - **version**: Numeric version number
  - **timestamp**: ISO 8601 formatted timestamp of the version

#### Example

```json
{"subject":"user:123","predicate":"name","indexedObject":"john_doe","object":"John Doe","partition":"0","versionDetail":{"version":1,"timestamp":"2024-01-15T10:30:00Z"}}
{"subject":"user:123","predicate":"email","indexedObject":"john@example.com","object":"john@example.com","partition":"0","versionDetail":{"version":1,"timestamp":"2024-01-15T10:30:00Z"}}
{"subject":"user:123","predicate":"age","indexedObject":"30","object":"30","partition":"0","versionDetail":null}
```

### File Sorting and Organization

To maximize git merge success, triples within a file should be sorted by a composite key:

```
Sort Order: partition → subject → predicate → indexedObject
```

This ordering ensures:
- Related triples (same subject) are grouped together
- Concurrent additions to different subjects don't conflict
- Changes to the same subject are localized

### Special Considerations

#### Timestamps and Conflicts

- Timestamps should use UTC timezone
- When merging conflicts with different timestamps, the later timestamp wins by default
- Version numbers should be monotonically increasing

#### Encoding

- Files must use UTF-8 encoding
- Line endings should be LF (Unix style) for consistency across platforms
- No byte order mark (BOM)

#### File Size Limits

- Individual partition files should be kept under 100MB for reasonable git performance
- Larger datasets should be split across multiple partition files

## Git Merge Scenarios

### Scenario 1: Non-Conflicting Additions

**Branch A adds:**
```json
{"subject":"user:124","predicate":"name","indexedObject":"jane_smith","object":"Jane Smith","partition":"0","versionDetail":null}
```

**Branch B adds:**
```json
{"subject":"user:125","predicate":"name","indexedObject":"bob_jones","object":"Bob Jones","partition":"0","versionDetail":null}
```

**Result**: Both additions are automatically merged (different subjects, different lines)

### Scenario 2: Conflicting Updates

**Base:**
```json
{"subject":"user:123","predicate":"email","indexedObject":"old@example.com","object":"old@example.com","partition":"0","versionDetail":{"version":1,"timestamp":"2024-01-15T10:00:00Z"}}
```

**Branch A:**
```json
{"subject":"user:123","predicate":"email","indexedObject":"new1@example.com","object":"new1@example.com","partition":"0","versionDetail":{"version":2,"timestamp":"2024-01-15T11:00:00Z"}}
```

**Branch B:**
```json
{"subject":"user:123","predicate":"email","indexedObject":"new2@example.com","object":"new2@example.com","partition":"0","versionDetail":{"version":2,"timestamp":"2024-01-15T11:30:00Z"}}
```

**Result**: Git conflict requiring manual resolution. The resolver should choose the triple with the later timestamp (Branch B in this case).

### Scenario 3: Delete and Modify

**Branch A deletes** a triple (removes the line)

**Branch B modifies** the same triple

**Result**: Git conflict. Resolution depends on business logic - typically the delete wins unless the modification is critical.

## Compatibility with InMemoryRDFTripleStore

The file format is designed to be compatible with the existing `InMemoryRDFTripleStore` data structures:

- **RDFTriple record**: Maps directly to JSON structure
- **Partitions**: File-per-partition approach mirrors in-memory partitions
- **Indexes**: Maintained in separate index files (see storage structure design)

## Migration Path

1. **Export**: Existing in-memory data can be serialized to JSONL format
2. **Import**: JSONL files can be loaded into in-memory structures
3. **Hybrid**: Support both storage types simultaneously during transition

## Validation

Each line must be:
- Valid JSON
- Conform to the RDF triple schema
- Have all required fields (subject, predicate, indexedObject, object, partition)

Invalid lines should be logged and skipped during loading, with appropriate error reporting.

## Future Considerations

- **Compression**: Consider gzip compression for archived or read-only partitions
- **Compaction**: Periodic compaction to remove deleted/superseded triples
- **Binary Format**: For performance-critical scenarios, a binary format could be added as an alternative
- **Schema Evolution**: Version field in each triple allows for future schema changes

## References

- [JSON Lines Format](https://jsonlines.org/)
- [Git Merge Documentation](https://git-scm.com/docs/git-merge)
- InMemoryRDFTripleStore implementation (src/GraphlessDB/Storage.Services.Internal.InMemory/)
