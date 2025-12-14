# GraphlessDB

GraphlessDB is an open source serverless graph database built upon AWS DynamoDB.

## Architecture

GraphlessDB is organized into a clean layered architecture with clear separation of concerns and one-way dependencies:

### Layer 0: Foundation (GraphlessDB.Core)
Core utilities and extensions with no domain dependencies:
- Collections (immutable lists, dictionaries, trees)
- Threading utilities (retry, lock)
- LINQ extensions

### Layer 1: Storage Abstractions (GraphlessDB.Storage)
Storage contracts and models:
- RDF triple models and predicates
- Storage interfaces (`IRDFTripleStore`, `IMemoryCache`)
- Request/response DTOs
- Base exceptions

### Layer 2: Storage Implementations
Pluggable storage providers (each in separate project):
- **GraphlessDB.Storage.InMemory** - In-memory storage for development/testing
- **GraphlessDB.Storage.FileBased** - File-based persistence
- **GraphlessDB.DynamoDB** - Production-grade AWS DynamoDB backend with optional transactions

### Layer 3: Domain (GraphlessDB.Domain)
Graph-specific domain logic:
- Domain models (`INode`, `IEdge`)
- Cursor types for pagination
- Graph services (partitioning, serialization, schema)
- Domain exceptions

### Layer 4: Query (GraphlessDB.Query)
Query execution layer:
- Query models and filters
- Query execution services
- Connection queries (fluent API)
- Graph query executors

### Layer 5: Public API (GraphlessDB)
Clean public-facing API:
- `IGraphDB` and `GraphDB` main interfaces
- Dependency injection extensions
- Service registration

### Dependency Graph

```
GraphlessDB (Public API)
    └── GraphlessDB.Query
        └── GraphlessDB.Domain
            └── GraphlessDB.Storage ← GraphlessDB.Storage.InMemory
                └── GraphlessDB.Core  ← GraphlessDB.Storage.FileBased
                                      ← GraphlessDB.DynamoDB
```

All dependencies flow downward (one-way), ensuring clean separation and easy testability.
