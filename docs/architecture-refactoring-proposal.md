# GraphlessDB Architecture Refactoring Proposal

**Date**: 2025-12-13
**Status**: Proposed
**Author**: Architecture Analysis

## Executive Summary

This document proposes a refactoring of the GraphlessDB codebase to establish clearer separation of concerns, reduce coupling, and enforce one-way dependencies between namespaces. The current architecture is well-designed but suffers from deep namespace hierarchies and some circular-like dependencies that make the codebase harder to maintain and extend.

---

## Current Architecture Analysis

### Project Structure

The solution currently contains **5 projects**:

#### Core Projects:
- **GraphlessDB** - Main library (net10.0, ~19,433 lines of code)
- **GraphlessDB.DynamoDB** - DynamoDB storage provider (net10.0)
- **GraphlessDB.Analyzers** - Roslyn source generators (netstandard2.0)

#### Test Projects:
- **GraphlessDB.Tests** - Tests for core library (net10.0)
- **GraphlessDB.DynamoDB.Tests** - Tests for DynamoDB provider (net10.0)

### Current Namespace Organization in GraphlessDB (Core)

#### Top-Level Namespaces (Public API):
- **GraphlessDB** (root) - Public API: IGraphDB, GraphDB, INode, IEdge, fluent query builders, filters, exceptions

#### Storage Layer:
- **GraphlessDB.Storage** - Storage models: RDFTriple, predicates (HasType, HasProp, HasInEdge, etc.), request/response objects
- **GraphlessDB.Storage.Services** - Storage interfaces: IRDFTripleStore, IRDFTripleKeyValueStore, IMemoryCache, IRDFTripleIntegrityChecker
- **GraphlessDB.Storage.Services.Internal** - Storage implementations: RDFTripleStore (facade), CachedRDFTripleStore, ConcurrentMemoryCache
- **GraphlessDB.Storage.Services.Internal.InMemory** - In-memory storage: InMemoryRDFTripleStore, InMemoryRDFEventReader, InMemoryNodeEventProcessor
- **GraphlessDB.Storage.Services.Internal.FileBased** - File-based storage: FileBasedRDFTripleStore, FileBasedRDFEventReader, FileBasedNodeEventProcessor

#### Query Layer:
- **GraphlessDB.Query** - Query models: SingleNodeQuery, NodeConnectionQuery, WhereNodeConnectionQuery, GraphResult, etc.
- **GraphlessDB.Query.Services** - Query interfaces: IGraphQueryExecutionService, IGraphNodeFilterService, IGraphEdgeFilterService, IGraphHouseKeepingService
- **GraphlessDB.Query.Services.Internal** - Query executors: NodeByIdQueryExecutor, FromEdgeConnectionQueryExecutor, ToEdgeConnectionQueryExecutor, etc. (20+ executors)

#### Graph Layer:
- **GraphlessDB.Graph** - Graph models: Cursor types, GetConnectionRequest/Response, IndexedCursor, HasTypeCursor, VersionedNodeKey
- **GraphlessDB.Graph.Services** - Graph interfaces: IRDFTripleFactory, IGraphEntityTypeService, IGraphPartitionService, IGraphSchemaService, IGraphSettingsService
- **GraphlessDB.Graph.Services.Internal** - Graph implementations: RDFTripleFactory, RDFTripleGraphQueryService, GraphSerializationService, GraphCursorSerializationService, GraphPartitionService

#### Utilities:
- **GraphlessDB.Collections** - Immutable collections: ImmutableGraph, ImmutableNodeList, ImmutableEdgeList, ImmutableTree
- **GraphlessDB.Collections.Generic** - Generic utilities: FuncEqualityComparer
- **GraphlessDB.Collections.Immutable** - Immutable utilities: ImmutableListSequence, ImmutableDictionarySequence
- **GraphlessDB.Threading** - Concurrency: Retry, Lock, RetryConditionRequest
- **GraphlessDB.Linq** - LINQ extensions
- **GraphlessDB.Logging** - Logging utilities
- **GraphlessDB.DependencyInjection** - Service registration: AddGraphlessDBWithInMemoryDB, AddGraphlessDBWithFileBasedDB, AddGraphlessDBCore

### Current Namespace Organization in GraphlessDB.DynamoDB

#### Storage Provider:
- **GraphlessDB.Storage.Services.DynamoDB** - DynamoDB implementation: AmazonDynamoDBRDFTripleStore, AmazonDynamoDBRDFTripleItemService, AmazonDynamoDBRDFTripleIntegrityChecker

#### DynamoDB Utilities:
- **GraphlessDB.DynamoDB** - DynamoDB utilities: AmazonDynamoDBKeyService, TableSchemaService, AttributeValueFactory, BatchWriteItemRequestExtensions

#### Transaction Support:
- **GraphlessDB.DynamoDB.Transactions** - Public transaction API: Transaction, IsolationLevel, TransactionState, IAmazonDynamoDBWithTransactions, exceptions
- **GraphlessDB.DynamoDB.Transactions.Internal** - Internal transaction implementation: AmazonDynamoDBWithTransactions, IsolatedGetItemService implementations, TransactionServiceEvents
- **GraphlessDB.DynamoDB.Transactions.Storage** - Transaction storage: TransactionStore, ItemImageStore, VersionedItemStore, RequestService

### Current Dependencies Between Projects

```
GraphlessDB.DynamoDB.Tests
    └── GraphlessDB.DynamoDB
    └── GraphlessDB.Tests (reuses test utilities)

GraphlessDB.DynamoDB
    └── GraphlessDB (project reference)
    └── AWSSDK.DynamoDBv2 (NuGet)

GraphlessDB.Tests
    └── GraphlessDB (project reference)
    └── GraphlessDB.Analyzers (analyzer reference)

GraphlessDB
    └── Microsoft.Extensions.Options (NuGet)
    └── Microsoft.Extensions.Logging.Abstractions (NuGet)

GraphlessDB.Analyzers
    └── Microsoft.CodeAnalysis.CSharp (NuGet)
    └── (No project dependencies)
```

### Internal Namespace Dependencies (within GraphlessDB)

```
Root (GraphlessDB)
    └── Query.Services (IGraphQueryExecutionService, IGraphNodeFilterService)
    └── Storage (RDFTriple, predicates)
    └── Graph.Services (IRDFTripleFactory)

Query.Services
    └── Query.Services.Internal (implementations)

Query.Services.Internal
    └── Graph.Services.Internal (RDFTripleGraphQueryService)
    └── Storage.Services (IRDFTripleStore)

Graph.Services.Internal
    └── Storage.Services (IRDFTripleStore)
    └── Graph.Services (interfaces)

Storage.Services.Internal
    └── Storage.Services.Internal.InMemory (InMemoryRDFTripleStore)
    └── Storage.Services.Internal.FileBased (FileBasedRDFTripleStore)
```

---

## Problems Identified

### 1. Unclear Dependency Direction
Query.Services.Internal → Graph.Services.Internal → Storage.Services creates circular-like dependencies that are resolved through dependency injection but make the architecture harder to understand.

### 2. Deep Namespace Hierarchies
Some namespaces are 5+ levels deep (`GraphlessDB.Storage.Services.Internal.InMemory`), making them cumbersome to work with and obscuring the actual architecture.

### 3. Mixed Concerns
The Graph, Query, and Storage layers are intertwined, with query executors depending on graph services which depend on storage services, creating tight coupling.

### 4. Internal Coupling
Heavy use of `*.Internal` namespaces couples implementations tightly within the assembly, making it harder to extract or replace components.

### 5. Storage Provider Coupling
While DynamoDB is in a separate project (good), InMemory and FileBased storage are deeply embedded in the main assembly, making it harder to evolve them independently.

---

## Proposed Architecture

### Layer-Based Structure

The proposed architecture separates the codebase into distinct layers with clear, one-way dependencies:

```
src/
├── GraphlessDB.Core/                          # Layer 0: Foundation
│   ├── Collections/
│   ├── Threading/
│   ├── Linq/
│   └── Logging/
│
├── GraphlessDB.Storage/                       # Layer 1: Storage Abstractions
│   ├── Models/                                # RDFTriple, Predicates
│   ├── Interfaces/                            # IRDFTripleStore, IMemoryCache
│   └── Requests/                              # Request/Response objects
│
├── GraphlessDB.Storage.InMemory/              # Layer 2: Storage Implementation
│   └── Internal/                              # InMemoryRDFTripleStore, indexes
│
├── GraphlessDB.Storage.FileBased/             # Layer 2: Storage Implementation
│   └── Internal/                              # FileBasedRDFTripleStore
│
├── GraphlessDB.Domain/                        # Layer 3: Domain/Graph Logic
│   ├── Models/                                # INode, IEdge, Cursor types
│   ├── Services/                              # IRDFTripleFactory, IGraphPartitionService
│   └── Internal/                              # Implementations
│
├── GraphlessDB.Query/                         # Layer 4: Query Layer
│   ├── Models/                                # Query types, GraphResult
│   ├── Executors/                             # Query executors (strategy pattern)
│   └── Services/                              # IGraphQueryExecutionService
│
├── GraphlessDB/                               # Layer 5: Public API
│   ├── IGraphDB.cs
│   ├── GraphDB.cs
│   ├── Builders/                              # Fluent query builders
│   ├── Filters/                               # Filter builders
│   └── DependencyInjection/                   # Service registration
│
├── GraphlessDB.DynamoDB/                      # Extension: External Storage Provider
│   ├── Storage/                               # AmazonDynamoDBRDFTripleStore
│   ├── Transactions/                          # Transaction system
│   └── DependencyInjection/
│
└── GraphlessDB.Analyzers/                     # Tool: Source Generators
```

### Namespace Mapping

#### Layer 0: Foundation (GraphlessDB.Core)
```
Namespaces:
- GraphlessDB.Collections
- GraphlessDB.Collections.Immutable
- GraphlessDB.Threading
- GraphlessDB.Linq
- GraphlessDB.Logging

Dependencies: None (Microsoft.Extensions.* only)

Purpose: Reusable utilities with no domain knowledge
```

#### Layer 1: Storage Abstractions (GraphlessDB.Storage)
```
Namespaces:
- GraphlessDB.Storage                          # RDFTriple, Predicates
- GraphlessDB.Storage.Interfaces               # IRDFTripleStore<T>, IMemoryCache
- GraphlessDB.Storage.Requests                 # Request/Response DTOs

Dependencies: GraphlessDB.Core → (one-way)

Purpose: Define storage contracts, no implementations
Current files: RDFTriple.cs, Predicates/*.cs, IRDFTripleStore.cs
```

#### Layer 2: Storage Implementations (Separate Projects)
```
Project: GraphlessDB.Storage.InMemory
Namespace: GraphlessDB.Storage.InMemory
Dependencies: GraphlessDB.Storage, GraphlessDB.Core → (one-way)
Current files: InMemoryRDFTripleStore.cs, InMemoryRDFTripleStoreTable.cs, etc.

Project: GraphlessDB.Storage.FileBased
Namespace: GraphlessDB.Storage.FileBased
Dependencies: GraphlessDB.Storage, GraphlessDB.Core → (one-way)
Current files: FileBasedRDFTripleStore.cs, FileBasedRDFEventReader.cs, etc.

Purpose: Concrete storage implementations
Note: Each storage provider is isolated, no cross-dependencies
```

#### Layer 3: Domain/Graph (GraphlessDB.Domain)
```
Namespaces:
- GraphlessDB.Domain                           # INode, IEdge, core graph types
- GraphlessDB.Domain.Cursors                   # Cursor types
- GraphlessDB.Domain.Services                  # IRDFTripleFactory, IGraphPartitionService
- GraphlessDB.Domain.Internal                  # RDFTripleFactory, GraphSerializationService

Dependencies: GraphlessDB.Storage, GraphlessDB.Core → (one-way)

Purpose: Graph-specific domain logic and services
Current files: RDFTripleFactory.cs, GraphPartitionService.cs, Cursor types
Note: Does NOT depend on Query layer
```

#### Layer 4: Query (GraphlessDB.Query)
```
Namespaces:
- GraphlessDB.Query                            # Query models (SingleNodeQuery, etc.)
- GraphlessDB.Query.Services                   # IGraphQueryExecutionService
- GraphlessDB.Query.Executors                  # All query executors
- GraphlessDB.Query.Filters                    # Filter services

Dependencies: GraphlessDB.Domain, GraphlessDB.Storage, GraphlessDB.Core → (one-way)

Purpose: Query execution and filtering
Current files: All query executors, IGraphQueryExecutionService.cs
Note: Depends on Domain for factories and services
```

#### Layer 5: Public API (GraphlessDB)
```
Namespaces:
- GraphlessDB                                  # IGraphDB, GraphDB, exceptions
- GraphlessDB.Builders                         # Fluent query builders
- GraphlessDB.Filters                          # Fluent filter builders
- GraphlessDB.DependencyInjection              # Service registration

Dependencies: GraphlessDB.Query, GraphlessDB.Domain, GraphlessDB.Storage.* → (one-way)

Purpose: Public-facing API
Current files: IGraphDB.cs, GraphDB.cs, builders, filters
```

#### Extension: DynamoDB Provider (GraphlessDB.DynamoDB)
```
Namespaces:
- GraphlessDB.Storage.DynamoDB                 # AmazonDynamoDBRDFTripleStore
- GraphlessDB.DynamoDB.Transactions            # Transaction system
- GraphlessDB.DynamoDB.Internal                # Internal utilities
- GraphlessDB.DynamoDB.DependencyInjection     # DI registration

Dependencies: GraphlessDB.Storage, GraphlessDB.Core → (one-way)

Purpose: External storage provider with optional transactions
Note: Depends ONLY on Storage abstractions, not Domain or Query
```

### Dependency Graph (One-Way Only)

```
                    ┌─────────────────┐
                    │  GraphlessDB    │  (Public API)
                    │  (Layer 5)      │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │ GraphlessDB     │  (Query Execution)
                    │ .Query          │
                    │ (Layer 4)       │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │ GraphlessDB     │  (Domain Logic)
                    │ .Domain         │
                    │ (Layer 3)       │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
┌───────▼────────┐  ┌────────▼────────┐  ┌───────▼────────┐
│ GraphlessDB    │  │ GraphlessDB     │  │ GraphlessDB    │
│ .Storage       │  │ .Storage        │  │ .Storage       │
│ .InMemory      │  │ .FileBased      │  │ .DynamoDB      │
│ (Layer 2)      │  │ (Layer 2)       │  │ (Extension)    │
└───────┬────────┘  └────────┬────────┘  └───────┬────────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
                    ┌────────▼────────┐
                    │ GraphlessDB     │  (Storage Contracts)
                    │ .Storage        │
                    │ (Layer 1)       │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │ GraphlessDB     │  (Foundation)
                    │ .Core           │
                    │ (Layer 0)       │
                    └─────────────────┘
```

---

## Benefits of Proposed Architecture

### 1. Clear Dependency Direction
All dependencies flow downward (one-way), making the architecture easier to understand and maintain.

### 2. Reduced Coupling
Storage implementations are isolated in separate projects with no cross-dependencies, allowing independent evolution.

### 3. Improved Testability
Each layer can be tested independently with clear boundaries for mocking and stubbing.

### 4. Better Extensibility
New storage providers only depend on Layer 1 (Storage abstractions), making it easy to add new backends.

### 5. Simplified Namespaces
Reduced from 5+ levels to 2-3 levels maximum, improving code navigation and readability.

### 6. Separation of Concerns
Domain logic is separate from query execution, which is separate from storage, following the Single Responsibility Principle.

### 7. Easier Onboarding
New developers can understand the architecture by following the layer structure from top to bottom.

---

## Migration Strategy

This migration is divided into stages that can be completed incrementally. Each stage must compile and pass all tests before proceeding to the next.

### Stage 0: Preparation
- [ ] Create this architecture document in `./docs`
- [ ] Review and approve proposed architecture
- [ ] Create migration tracking branch
- [ ] Ensure all existing tests pass on current main branch
- [ ] Document current test coverage baseline

### Stage 1: Create GraphlessDB.Core Project
**Goal**: Extract foundation utilities into separate project with no domain dependencies

- [ ] Create new project: `src/GraphlessDB.Core/GraphlessDB.Core.csproj` (net10.0)
- [ ] Move `GraphlessDB.Collections` namespace to GraphlessDB.Core
- [ ] Move `GraphlessDB.Collections.Generic` namespace to GraphlessDB.Core
- [ ] Move `GraphlessDB.Collections.Immutable` namespace to GraphlessDB.Core
- [ ] Move `GraphlessDB.Threading` namespace to GraphlessDB.Core
- [ ] Move `GraphlessDB.Linq` namespace to GraphlessDB.Core
- [ ] Move `GraphlessDB.Logging` namespace to GraphlessDB.Core
- [ ] Add project reference from GraphlessDB to GraphlessDB.Core
- [ ] Update all internal references to use GraphlessDB.Core
- [ ] Run `dotnet build` - ensure solution compiles
- [ ] Run `dotnet test` - ensure all tests pass
- [ ] Commit: "Stage 1: Extract GraphlessDB.Core foundation utilities"

**Validation Checkpoint**: Solution compiles, all tests pass

### Stage 2: Create GraphlessDB.Storage Abstractions
**Goal**: Extract storage abstractions (interfaces and models only, no implementations)

- [ ] Create new project: `src/GraphlessDB.Storage/GraphlessDB.Storage.csproj` (net10.0)
- [ ] Add project reference from GraphlessDB.Storage to GraphlessDB.Core
- [ ] Move `RDFTriple.cs` to GraphlessDB.Storage
- [ ] Move all predicate types to `GraphlessDB.Storage/Predicates/`
- [ ] Create `GraphlessDB.Storage.Interfaces` namespace
- [ ] Move `IRDFTripleStore.cs` to GraphlessDB.Storage.Interfaces
- [ ] Move `IRDFTripleKeyValueStore.cs` to GraphlessDB.Storage.Interfaces
- [ ] Move `IMemoryCache.cs` to GraphlessDB.Storage.Interfaces
- [ ] Move `IRDFTripleIntegrityChecker.cs` to GraphlessDB.Storage.Interfaces
- [ ] Create `GraphlessDB.Storage.Requests` namespace
- [ ] Move all Request/Response DTOs to GraphlessDB.Storage.Requests
- [ ] Add project reference from GraphlessDB to GraphlessDB.Storage
- [ ] Update all internal references in GraphlessDB project
- [ ] Run `dotnet build` - ensure solution compiles
- [ ] Run `dotnet test` - ensure all tests pass
- [ ] Commit: "Stage 2: Extract GraphlessDB.Storage abstractions"

**Validation Checkpoint**: Solution compiles, all tests pass

### Stage 3: Create GraphlessDB.Storage.InMemory Project
**Goal**: Extract in-memory storage implementation to separate project

- [ ] Create new project: `src/GraphlessDB.Storage.InMemory/GraphlessDB.Storage.InMemory.csproj` (net10.0)
- [ ] Add project reference to GraphlessDB.Storage
- [ ] Add project reference to GraphlessDB.Core
- [ ] Move all files from `Storage.Services.Internal.InMemory` to new project
- [ ] Update namespace to `GraphlessDB.Storage.InMemory.Internal`
- [ ] Move `InMemoryRDFTripleStore.cs`
- [ ] Move `InMemoryRDFTripleStoreTable.cs`
- [ ] Move `InMemoryRDFTripleStoreIndex.cs`
- [ ] Move `InMemoryRDFTripleStorePartition.cs`
- [ ] Move `InMemoryRDFEventReader.cs`
- [ ] Move `InMemoryNodeEventProcessor.cs`
- [ ] Move all related helper classes
- [ ] Add project reference from GraphlessDB to GraphlessDB.Storage.InMemory
- [ ] Update DI registration in `GraphlessDB.DependencyInjection`
- [ ] Run `dotnet build` - ensure solution compiles
- [ ] Run `dotnet test` - ensure all tests pass
- [ ] Commit: "Stage 3: Extract GraphlessDB.Storage.InMemory"

**Validation Checkpoint**: Solution compiles, all tests pass

### Stage 4: Create GraphlessDB.Storage.FileBased Project
**Goal**: Extract file-based storage implementation to separate project

- [ ] Create new project: `src/GraphlessDB.Storage.FileBased/GraphlessDB.Storage.FileBased.csproj` (net10.0)
- [ ] Add project reference to GraphlessDB.Storage
- [ ] Add project reference to GraphlessDB.Core
- [ ] Move all files from `Storage.Services.Internal.FileBased` to new project
- [ ] Update namespace to `GraphlessDB.Storage.FileBased.Internal`
- [ ] Move `FileBasedRDFTripleStore.cs`
- [ ] Move `FileBasedRDFEventReader.cs`
- [ ] Move `FileBasedNodeEventProcessor.cs`
- [ ] Move `FileBasedRDFTripleStoreOptions.cs`
- [ ] Move all related helper classes
- [ ] Add project reference from GraphlessDB to GraphlessDB.Storage.FileBased
- [ ] Update DI registration in `GraphlessDB.DependencyInjection`
- [ ] Run `dotnet build` - ensure solution compiles
- [ ] Run `dotnet test` - ensure all tests pass
- [ ] Commit: "Stage 4: Extract GraphlessDB.Storage.FileBased"

**Validation Checkpoint**: Solution compiles, all tests pass

### Stage 5: Refactor Remaining Storage Services
**Goal**: Clean up remaining storage services in main GraphlessDB project

- [ ] Move `RDFTripleStore.cs` (facade) to GraphlessDB.Storage.Interfaces
- [ ] Move `CachedRDFTripleStore.cs` to GraphlessDB.Storage
- [ ] Move `ConcurrentMemoryCache.cs` to GraphlessDB.Storage.InMemory
- [ ] Update all Storage.Services.Internal references to new locations
- [ ] Remove empty `Storage.Services.Internal` folders
- [ ] Run `dotnet build` - ensure solution compiles
- [ ] Run `dotnet test` - ensure all tests pass
- [ ] Commit: "Stage 5: Refactor remaining storage services"

**Validation Checkpoint**: Solution compiles, all tests pass

### Stage 6: Create GraphlessDB.Domain Project
**Goal**: Extract domain/graph logic into separate layer

- [ ] Create new project: `src/GraphlessDB.Domain/GraphlessDB.Domain.csproj` (net10.0)
- [ ] Add project reference to GraphlessDB.Storage
- [ ] Add project reference to GraphlessDB.Core
- [ ] Move `INode.cs`, `IEdge.cs` from root to GraphlessDB.Domain
- [ ] Create `GraphlessDB.Domain.Cursors` namespace
- [ ] Move all cursor types from `Graph` namespace to Domain.Cursors
- [ ] Create `GraphlessDB.Domain.Services` namespace
- [ ] Move `IRDFTripleFactory.cs` to Domain.Services
- [ ] Move `IGraphEntityTypeService.cs` to Domain.Services
- [ ] Move `IGraphPartitionService.cs` to Domain.Services
- [ ] Move `IGraphSchemaService.cs` to Domain.Services
- [ ] Move `IGraphSettingsService.cs` to Domain.Services
- [ ] Create `GraphlessDB.Domain.Internal` namespace
- [ ] Move `RDFTripleFactory.cs` to Domain.Internal
- [ ] Move `GraphSerializationService.cs` to Domain.Internal
- [ ] Move `GraphCursorSerializationService.cs` to Domain.Internal
- [ ] Move `GraphPartitionService.cs` to Domain.Internal
- [ ] Move other graph service implementations to Domain.Internal
- [ ] Add project reference from GraphlessDB to GraphlessDB.Domain
- [ ] Update all references in GraphlessDB project
- [ ] Run `dotnet build` - ensure solution compiles
- [ ] Run `dotnet test` - ensure all tests pass
- [ ] Commit: "Stage 6: Extract GraphlessDB.Domain"

**Validation Checkpoint**: Solution compiles, all tests pass

### Stage 7: Create GraphlessDB.Query Project
**Goal**: Extract query layer into separate project

- [ ] Create new project: `src/GraphlessDB.Query/GraphlessDB.Query.csproj` (net10.0)
- [ ] Add project reference to GraphlessDB.Domain
- [ ] Add project reference to GraphlessDB.Storage
- [ ] Add project reference to GraphlessDB.Core
- [ ] Move all query model types from `Query` namespace to GraphlessDB.Query
- [ ] Create `GraphlessDB.Query.Services` namespace
- [ ] Move `IGraphQueryExecutionService.cs` to Query.Services
- [ ] Move `IGraphNodeFilterService.cs` to Query.Services
- [ ] Move `IGraphEdgeFilterService.cs` to Query.Services
- [ ] Move `IGraphHouseKeepingService.cs` to Query.Services
- [ ] Create `GraphlessDB.Query.Executors` namespace
- [ ] Move all executor classes from `Query.Services.Internal` to Query.Executors
- [ ] Update namespace from `*.Internal.*Executor` to `*.Executors.*Executor`
- [ ] Create `GraphlessDB.Query.Filters` namespace
- [ ] Move filter service implementations to Query.Filters
- [ ] Add project reference from GraphlessDB to GraphlessDB.Query
- [ ] Update all references in GraphlessDB project
- [ ] Run `dotnet build` - ensure solution compiles
- [ ] Run `dotnet test` - ensure all tests pass
- [ ] Commit: "Stage 7: Extract GraphlessDB.Query"

**Validation Checkpoint**: Solution compiles, all tests pass

### Stage 8: Refactor GraphlessDB Public API
**Goal**: Slim down main GraphlessDB project to be a thin public API layer

- [ ] Verify `IGraphDB.cs` and `GraphDB.cs` remain in root namespace
- [ ] Organize builder classes under `GraphlessDB.Builders`
- [ ] Organize filter classes under `GraphlessDB.Filters`
- [ ] Keep DI registration in `GraphlessDB.DependencyInjection`
- [ ] Remove any remaining `*.Internal` namespaces in public API
- [ ] Ensure GraphlessDB only contains public-facing types
- [ ] Update project references to include all layer dependencies
- [ ] Run `dotnet build` - ensure solution compiles
- [ ] Run `dotnet test` - ensure all tests pass
- [ ] Commit: "Stage 8: Refactor GraphlessDB public API"

**Validation Checkpoint**: Solution compiles, all tests pass

### Stage 9: Update GraphlessDB.DynamoDB
**Goal**: Ensure DynamoDB provider only depends on Storage abstractions

- [ ] Review project references in GraphlessDB.DynamoDB
- [ ] Ensure it references GraphlessDB.Storage (not full GraphlessDB)
- [ ] Ensure it references GraphlessDB.Core
- [ ] Update namespace from `Storage.Services.DynamoDB` to `Storage.DynamoDB`
- [ ] Move DynamoDB-specific utilities to `GraphlessDB.DynamoDB.Internal`
- [ ] Verify transaction system namespaces are clean
- [ ] Update DI registration for new structure
- [ ] Run `dotnet build` - ensure solution compiles
- [ ] Run `dotnet test` - ensure all tests pass (including DynamoDB tests)
- [ ] Commit: "Stage 9: Update GraphlessDB.DynamoDB dependencies"

**Validation Checkpoint**: Solution compiles, all tests pass (including DynamoDB)

### Stage 10: Update Test Projects
**Goal**: Update test projects to reference new project structure

- [ ] Update `GraphlessDB.Tests` project references
- [ ] Add references to new layer projects as needed
- [ ] Update test namespaces to match new structure
- [ ] Update `GraphlessDB.DynamoDB.Tests` project references
- [ ] Verify test utilities are accessible
- [ ] Run `dotnet build` - ensure solution compiles
- [ ] Run `dotnet test` - ensure all tests pass
- [ ] Verify test coverage has not decreased
- [ ] Commit: "Stage 10: Update test projects"

**Validation Checkpoint**: Solution compiles, all tests pass, coverage maintained

### Stage 11: Update Solution File and Build Configuration
**Goal**: Update solution file to include all new projects

- [ ] Add GraphlessDB.Core to solution
- [ ] Add GraphlessDB.Storage to solution
- [ ] Add GraphlessDB.Storage.InMemory to solution
- [ ] Add GraphlessDB.Storage.FileBased to solution
- [ ] Add GraphlessDB.Domain to solution
- [ ] Add GraphlessDB.Query to solution
- [ ] Organize projects in solution folders (Core, Storage, Domain, Query, Extensions, Tests)
- [ ] Update build order if needed
- [ ] Verify `dotnet build` at solution level works
- [ ] Verify `dotnet test` at solution level works
- [ ] Commit: "Stage 11: Update solution configuration"

**Validation Checkpoint**: Solution builds and all tests pass

### Stage 12: Documentation and Cleanup
**Goal**: Update documentation and clean up obsolete code

- [ ] Update README.md with new architecture
- [ ] Update package descriptions for all projects
- [ ] Remove any obsolete files or folders
- [ ] Update XML documentation comments if needed
- [ ] Create architecture diagram (optional)
- [ ] Update CHANGELOG.md
- [ ] Run `dotnet build` - final verification
- [ ] Run `dotnet test` - final verification
- [ ] Commit: "Stage 12: Update documentation and cleanup"

**Final Validation**: Solution compiles, all tests pass, documentation updated

### Stage 13: Review and Merge
**Goal**: Final review before merging to main

- [ ] Review all changes in migration branch
- [ ] Verify dependency graph matches proposal
- [ ] Run full test suite one final time
- [ ] Check for any performance regressions
- [ ] Get peer review/approval
- [ ] Merge migration branch to main
- [ ] Create release tag if appropriate
- [ ] Update this document status to "Completed"

---

## Rollback Plan

If any stage fails validation:

1. Do not proceed to next stage
2. Review errors and determine if they can be fixed within the current stage
3. If fixes are possible, make corrections and re-validate
4. If fixes are not feasible, consider:
   - Reverting the current stage
   - Adjusting the migration strategy
   - Seeking architectural guidance
5. Document any deviations from the plan in this document

---

## Success Criteria

The migration is considered successful when:

- [ ] All projects compile without errors
- [ ] All existing tests pass
- [ ] Test coverage is maintained or improved
- [ ] Dependency graph matches proposed architecture (one-way dependencies only)
- [ ] Namespaces are simplified (2-3 levels max)
- [ ] Each layer has clear responsibilities
- [ ] Storage providers are isolated in separate projects
- [ ] Public API (GraphlessDB) is a thin facade over lower layers
- [ ] Documentation is updated

---

## Post-Migration Improvements

After successful migration, consider:

1. **Package Structure**: Publish separate NuGet packages for each layer
2. **Performance Testing**: Benchmark to ensure no performance regression
3. **API Documentation**: Generate comprehensive API docs
4. **Example Projects**: Create examples showing new architecture
5. **Migration Guide**: Create guide for users upgrading from old structure

---

## Appendix: Namespace Mapping Reference

| Current Namespace | Proposed Namespace | New Project |
|-------------------|-------------------|-------------|
| `GraphlessDB.Collections` | `GraphlessDB.Collections` | GraphlessDB.Core |
| `GraphlessDB.Collections.Generic` | `GraphlessDB.Collections` | GraphlessDB.Core |
| `GraphlessDB.Collections.Immutable` | `GraphlessDB.Collections.Immutable` | GraphlessDB.Core |
| `GraphlessDB.Threading` | `GraphlessDB.Threading` | GraphlessDB.Core |
| `GraphlessDB.Linq` | `GraphlessDB.Linq` | GraphlessDB.Core |
| `GraphlessDB.Logging` | `GraphlessDB.Logging` | GraphlessDB.Core |
| `GraphlessDB.Storage` (models) | `GraphlessDB.Storage` | GraphlessDB.Storage |
| `GraphlessDB.Storage.Services` | `GraphlessDB.Storage.Interfaces` | GraphlessDB.Storage |
| `GraphlessDB.Storage.Services.Internal.InMemory` | `GraphlessDB.Storage.InMemory.Internal` | GraphlessDB.Storage.InMemory |
| `GraphlessDB.Storage.Services.Internal.FileBased` | `GraphlessDB.Storage.FileBased.Internal` | GraphlessDB.Storage.FileBased |
| `GraphlessDB.Graph` (models) | `GraphlessDB.Domain` | GraphlessDB.Domain |
| `GraphlessDB.Graph` (cursors) | `GraphlessDB.Domain.Cursors` | GraphlessDB.Domain |
| `GraphlessDB.Graph.Services` | `GraphlessDB.Domain.Services` | GraphlessDB.Domain |
| `GraphlessDB.Graph.Services.Internal` | `GraphlessDB.Domain.Internal` | GraphlessDB.Domain |
| `GraphlessDB.Query` | `GraphlessDB.Query` | GraphlessDB.Query |
| `GraphlessDB.Query.Services` | `GraphlessDB.Query.Services` | GraphlessDB.Query |
| `GraphlessDB.Query.Services.Internal` | `GraphlessDB.Query.Executors` | GraphlessDB.Query |
| `GraphlessDB` (root) | `GraphlessDB` | GraphlessDB |
| `GraphlessDB.Storage.Services.DynamoDB` | `GraphlessDB.Storage.DynamoDB` | GraphlessDB.DynamoDB |

---

**Document Version**: 1.0
**Last Updated**: 2025-12-13
