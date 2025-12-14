# GraphlessDB Architecture Refactoring Proposal

**Date**: 2025-12-13
**Status**: Implementation Complete - Documentation in Progress
**Author**: Architecture Analysis
**Completed**: 2025-12-14

## Executive Summary

This document proposes a refactoring of the GraphlessDB codebase to establish clearer separation of concerns, reduce coupling, and enforce one-way dependencies between namespaces. The current architecture is well-designed but suffers from deep namespace hierarchies and some circular-like dependencies that make the codebase harder to maintain and extend.

---

## Current Architecture Analysis

### Project Structure

The solution now contains **11 projects** (after refactoring):

#### Layer 0 - Foundation:
- **GraphlessDB.Core** - Foundation utilities (net10.0)

#### Layer 1 - Storage Abstractions:
- **GraphlessDB.Storage** - Storage contracts and models (net10.0)

#### Layer 2 - Storage Implementations:
- **GraphlessDB.Storage.InMemory** - In-memory storage provider (net10.0)
- **GraphlessDB.Storage.FileBased** - File-based storage provider (net10.0)
- **GraphlessDB.DynamoDB** - DynamoDB storage provider (net10.0)

#### Layer 3 - Domain:
- **GraphlessDB.Domain** - Domain models and services (net10.0)

#### Layer 4 - Query:
- **GraphlessDB.Query** - Query execution layer (net10.0)

#### Layer 5 - Public API:
- **GraphlessDB** - Public API facade (net10.0)

#### Tools:
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
- [x] Create this architecture document in `./docs`
- [x] Review and approve proposed architecture
- [x] Create migration tracking branch (`architecture-refactoring`)
- [x] Ensure all existing tests pass on current main branch (3,012 tests passed)
- [x] Document current test coverage baseline

**Baseline Metrics:**
- Total Tests: 3,012 (GraphlessDB.Tests: 2,094, GraphlessDB.DynamoDB.Tests: 918)
- Line Coverage: 59.65%
- Branch Coverage: 52.48%
- Build Status: Success (0 warnings, 0 errors)

### Stage 1: Create GraphlessDB.Core Project
**Goal**: Extract foundation utilities into separate project with no domain dependencies

- [x] Create new project: `src/GraphlessDB.Core/GraphlessDB.Core.csproj` (net10.0)
- [x] Move `GraphlessDB.Collections` namespace to GraphlessDB.Core
- [x] Move `GraphlessDB.Collections.Generic` namespace to GraphlessDB.Core
- [x] Move `GraphlessDB.Collections.Immutable` namespace to GraphlessDB.Core
- [x] Move `GraphlessDB.Threading` namespace to GraphlessDB.Core
- [x] Move `GraphlessDB.Linq` namespace to GraphlessDB.Core
- [x] ~~Move `GraphlessDB.Logging` namespace to GraphlessDB.Core~~ (NOT moved - has domain dependencies on RDFTriple)
- [x] Add project reference from GraphlessDB to GraphlessDB.Core
- [x] Add GraphlessDB.Core to solution file
- [x] Run `dotnet build` - ensure solution compiles
- [x] Run `dotnet test` - ensure all tests pass
- [x] Commit: "Stage 1: Extract GraphlessDB.Core foundation utilities"

**Validation Checkpoint**: ✅ Solution compiles (0 errors, 0 warnings), all 3,012 tests pass

**Note**: `GraphlessDB.Logging` was NOT moved to Core because it contains domain-specific logging methods that depend on `RDFTriple` and other storage types. It will remain in the main GraphlessDB project.

### Stage 2: Create GraphlessDB.Storage Abstractions
**Goal**: Extract storage abstractions (interfaces and models only, no implementations)

- [x] Create new project: `src/GraphlessDB.Storage/GraphlessDB.Storage.csproj` (net10.0)
- [x] Add project reference from GraphlessDB.Storage to GraphlessDB.Core
- [x] Move entire `Storage` directory to GraphlessDB.Storage (RDFTriple, predicates, all storage models)
- [x] Move `Storage.Services` interfaces to `GraphlessDB.Storage/Interfaces/`
- [x] Update namespace from `GraphlessDB.Storage.Services` to `GraphlessDB.Storage.Interfaces`
- [x] Move supporting types: `VersionDetail`, `PropertyOperator`, `PartitionPosition` to GraphlessDB.Storage
- [x] Move exception types: `GraphlessDBException`, `GraphlessDBOperationException` to GraphlessDB.Storage
- [x] Add project reference from GraphlessDB to GraphlessDB.Storage
- [x] Add project reference from GraphlessDB.DynamoDB to GraphlessDB (transitive to Storage)
- [x] Update all using statements across projects (GraphlessDB, Tests, DynamoDB)
- [x] Add GraphlessDB.Storage to solution file
- [x] Run `dotnet build` - ensure solution compiles
- [x] Run `dotnet test` - ensure all tests pass
- [x] Commit: "Stage 2: Extract GraphlessDB.Storage abstractions"

**Validation Checkpoint**: ✅ Solution compiles (0 errors, 0 warnings), all 3,012 tests pass

**Files Moved**:
- Storage models (47 files): RDFTriple, predicates (HasType, HasProp, Has*Edge, etc.), DTOs
- Storage interfaces (5 files): IRDFTripleStore, IRDFTripleKeyValueStore, IMemoryCache, etc.
- Supporting types: VersionDetail, PropertyOperator, PartitionPosition
- Exceptions: GraphlessDBException, GraphlessDBOperationException

### Stage 3: Create GraphlessDB.Storage.InMemory Project
**Goal**: Extract in-memory storage implementation to separate project

**⚠️ DEPENDENCY ISSUE DISCOVERED**: The InMemory storage implementation depends on domain types (INode, IGraphSettingsService, IRDFTripleFactory, GraphOptions) that are still in the main GraphlessDB project. We cannot extract storage implementations until the Domain layer is extracted first.

**RESOLUTION**: Skipped Stages 3-5 initially and proceeded to Stage 6 (Create GraphlessDB.Domain). After Domain was extracted, returned to complete Stages 3-5.

**Revised Migration Order**:
- ✅ Stages 0-2: Complete (Core, Storage abstractions)
- ✅ Stage 6: Created GraphlessDB.Domain first
- ✅ Stages 3-5: Extracted storage implementations after Domain exists

- [x] Create new project: `src/GraphlessDB.Storage.InMemory/GraphlessDB.Storage.InMemory.csproj` (net10.0)
- [x] Add project reference to GraphlessDB.Storage
- [x] Add project reference to GraphlessDB.Core
- [x] Add project reference to GraphlessDB.Domain (required for INode, IRDFTripleFactory, etc.)
- [x] Move all files from `Storage.Services.Internal.InMemory` to new project (11 files)
- [x] Keep namespace as `GraphlessDB.Storage.Services.Internal.InMemory` (unchanged for compatibility)
- [x] Move `InMemoryRDFTripleStore.cs`
- [x] Move `InMemoryRDFTripleStoreTable.cs`
- [x] Move `InMemoryRDFTripleStoreIndex.cs`
- [x] Move `InMemoryRDFTripleStorePartition.cs`
- [x] Move `InMemoryRDFTripleStoreIndexTable.cs`
- [x] Move `InMemoryRDFTripleStoreConsumedCapacity.cs`
- [x] Move `InMemoryRDFEventReader.cs`
- [x] Move `InMemoryNodeEventProcessor.cs`
- [x] Move `IInMemoryRDFEventReader.cs`
- [x] Move `IInMemoryNodeEventProcessor.cs`
- [x] Add project reference from GraphlessDB to GraphlessDB.Storage.InMemory
- [x] Add InternalsVisibleTo for GraphlessDB and GraphlessDB.Tests
- [x] Run `dotnet build` - ensure solution compiles
- [x] Run `dotnet test` - ensure all tests pass
- [x] Commit: "Stage 3: Extract GraphlessDB.Storage.InMemory"

**Validation Checkpoint**: ✅ Solution compiles (0 errors, 0 warnings), all 3,012 tests pass

### Stage 4: Create GraphlessDB.Storage.FileBased Project
**Goal**: Extract file-based storage implementation to separate project

- [x] Create new project: `src/GraphlessDB.Storage.FileBased/GraphlessDB.Storage.FileBased.csproj` (net10.0)
- [x] Add project reference to GraphlessDB.Storage
- [x] Add project reference to GraphlessDB.Core
- [x] Add project reference to GraphlessDB.Domain
- [x] Move all files from `Storage.Services.Internal.FileBased` to new project (7 files)
- [x] Keep namespace as `GraphlessDB.Storage.Services.Internal.FileBased` (unchanged for compatibility)
- [x] Move `FileBasedRDFTripleStore.cs`
- [x] Move `FileBasedRDFEventReader.cs`
- [x] Move `FileBasedNodeEventProcessor.cs`
- [x] Move `FileBasedRDFTripleStoreOptions.cs`
- [x] Move `FileBasedRDFTripleStoreConsumedCapacity.cs`
- [x] Move `IFileBasedRDFEventReader.cs`
- [x] Move `IFileBasedNodeEventProcessor.cs`
- [x] Add project reference from GraphlessDB to GraphlessDB.Storage.FileBased
- [x] Add InternalsVisibleTo for GraphlessDB and GraphlessDB.Tests
- [x] Run `dotnet build` - ensure solution compiles
- [x] Run `dotnet test` - ensure all tests pass
- [x] Commit: "Stage 4: Extract GraphlessDB.Storage.FileBased"

**Validation Checkpoint**: ✅ Solution compiles (0 errors, 0 warnings), all 3,012 tests pass

### Stage 5: Update References After Extraction
**Goal**: Update project references to use extracted storage implementations

- [x] Update `GraphlessDB/GraphlessDB.csproj` to reference GraphlessDB.Storage.InMemory
- [x] Update `GraphlessDB/GraphlessDB.csproj` to reference GraphlessDB.Storage.FileBased
- [x] Verify all storage implementations are accessible
- [x] Keep `RDFTripleStore.cs` (facade) in GraphlessDB (internal implementation)
- [x] Keep `CachedRDFTripleStore.cs` in GraphlessDB (internal implementation)
- [x] Keep `ConcurrentMemoryCache.cs` in GraphlessDB (internal implementation)
- [x] Run `dotnet build` - ensure solution compiles
- [x] Run `dotnet test` - ensure all tests pass
- [x] Commit: "Stages 3-5: Extract storage implementations"

**Validation Checkpoint**: ✅ Solution compiles (0 errors, 0 warnings), all 3,012 tests pass

**Note**: Remaining storage services (RDFTripleStore, CachedRDFTripleStore, ConcurrentMemoryCache) kept in GraphlessDB project as internal implementation details.

### Stage 6: Create GraphlessDB.Domain Project
**Goal**: Extract domain/graph logic into separate layer

- [x] Create new project: `src/GraphlessDB.Domain/GraphlessDB.Domain.csproj` (net10.0)
- [x] Add project reference to GraphlessDB.Storage
- [x] Add project reference to GraphlessDB.Core
- [x] Move entire `Graph/` directory to GraphlessDB.Domain (INode, IEdge, cursors, etc.)
- [x] Move all cursor types from `Graph` namespace (kept namespaces unchanged)
- [x] Move graph service interfaces and implementations
- [x] Move `IRDFTripleFactory.cs` and implementation
- [x] Move `IGraphEntityTypeService.cs` and implementation
- [x] Move `IGraphPartitionService.cs` and implementation
- [x] Move `IGraphSchemaService.cs` and implementation
- [x] Move `IGraphSettingsService.cs` and implementation
- [x] Move `IGraphCursorSerializationService.cs` and implementation
- [x] Move `RDFTripleFactory.cs` to Domain
- [x] Move `GraphSerializationService.cs` to Domain
- [x] Move `GraphCursorSerializationService.cs` to Domain
- [x] Move `GraphPartitionService.cs` to Domain
- [x] Move exception types: `GraphlessDBInvalidOperationException`, `GraphlessDBInvalidDataException`, `GraphlessDBInvalidSchemaException`, `GraphlessDBCursorSerializationException`, `IGraphlessDBRetriableException`
- [x] Add project reference from GraphlessDB to GraphlessDB.Domain
- [x] Add InternalsVisibleTo for Query and main GraphlessDB project
- [x] Update all using statements across projects
- [x] Run `dotnet build` - ensure solution compiles
- [x] Run `dotnet test` - ensure all tests pass
- [x] Commit: "Stage 6: Extract GraphlessDB.Domain"

**Validation Checkpoint**: ✅ Solution compiles (0 errors, 0 warnings), all 3,012 tests pass

### Stage 7: Create GraphlessDB.Query Project
**Goal**: Extract query layer into separate project

- [x] Create new project: `src/GraphlessDB.Query/GraphlessDB.Query.csproj` (net10.0)
- [x] Add project reference to GraphlessDB.Domain
- [x] Add project reference to GraphlessDB.Storage
- [x] Add project reference to GraphlessDB.Core
- [x] Move entire `Query/` directory to GraphlessDB.Query (queries, filters, extensions)
- [x] Move `Query.Services/` directory to GraphlessDB.Query/Services/
- [x] Move `Query.Services.Internal/` to GraphlessDB.Query/Internal/ (44+ executor files)
- [x] Move `IGraphQueryExecutionService.cs` to Query.Services
- [x] Move `IGraphNodeFilterService.cs` to Query.Services
- [x] Move `IGraphEdgeFilterService.cs` to Query.Services
- [x] Move `IGraphHouseKeepingService.cs` to Query.Services
- [x] Move all executor classes (kept namespaces unchanged for compatibility)
- [x] Move all filter types: GraphQuery, NodeFilter, EdgeFilter, DateTimeFilter, EnumFilter, StringFilter, IntFilter, IdFilter
- [x] Move filter interfaces: INodeFilter, IEdgeFilter, INodeOrder, IEdgeOrder
- [x] Move connection query types: FluentNodeConnectionQuery, FluentEdgeConnectionQuery
- [x] Move extensions: PageInfoExtensions, GraphQueryExtensions, GraphQueryItemExtensions
- [x] Move utility types: StringRange, EdgeOrDefaultByIdQuery, PutQuery
- [x] Move public interfaces: IGraph, IFluentQuery
- [x] Move domain exceptions: GraphlessDBConcurrencyException, GraphlessDBThroughputExceededException to GraphlessDB.Domain
- [x] Add InternalsVisibleTo for GraphlessDB and GraphlessDB.Tests
- [x] Add project reference from GraphlessDB to GraphlessDB.Query
- [x] Update all using statements using Python automation scripts
- [x] Run `dotnet build` - ensure solution compiles
- [x] Run `dotnet test` - ensure all tests pass
- [x] Commit: "Stage 7: Extract GraphlessDB.Query"

**Validation Checkpoint**: ✅ Solution compiles (0 errors, 0 warnings), all 3,012 tests pass

### Stage 8: Refactor GraphlessDB Public API
**Goal**: Slim down main GraphlessDB project to be a thin public API layer

- [x] Verify `IGraphDB.cs` and `GraphDB.cs` remain in root namespace
- [x] Move `ServiceCollectionExtensions.cs` from `DependencyInjection/` to root
- [x] Move `ServiceScopeExtensions.cs` from `DependencyInjection/` to root
- [x] Change namespace from `GraphlessDB.DependencyInjection` to `GraphlessDB`
- [x] Make `ServiceCollectionExtensions` public (was internal)
- [x] Make `ServiceScopeExtensions` public (was internal)
- [x] Add public `GraphDB()` extension method on `IServiceScope`
- [x] Make `FileBasedRDFTripleStoreOptions` public (was internal)
- [x] Move remaining exception types to GraphlessDB.Domain
- [x] Ensure GraphlessDB only contains public-facing types and DI setup
- [x] Update project references to include all layer dependencies
- [x] Run `dotnet build` - ensure solution compiles
- [x] Run `dotnet test` - ensure all tests pass
- [x] Commit: "Stage 8: Refactor GraphlessDB public API"

**Validation Checkpoint**: ✅ Solution compiles (0 errors, 0 warnings), all 3,012 tests pass

### Stage 9: Update GraphlessDB.DynamoDB
**Goal**: Ensure DynamoDB provider dependencies are updated for new architecture

- [x] Review project references in GraphlessDB.DynamoDB
- [x] Add explicit reference to GraphlessDB.Core
- [x] Add explicit reference to GraphlessDB.Storage
- [x] Add explicit reference to GraphlessDB.Domain
- [x] Keep reference to GraphlessDB (needed for AddGraphlessDBCore method)
- [x] Verify namespaces are correct (kept unchanged for compatibility)
- [x] Verify transaction system compiles correctly
- [x] Run `dotnet build` - ensure solution compiles
- [x] Run `dotnet test` - ensure all tests pass (including DynamoDB tests)
- [x] Commit: "Stage 9: Update GraphlessDB.DynamoDB"

**Validation Checkpoint**: ✅ Solution compiles (0 errors, 0 warnings), all 3,012 tests pass

**Note**: DynamoDB namespaces kept unchanged. Project now has explicit dependencies on all architectural layers it needs.

### Stage 10: Update Test Projects
**Goal**: Update test projects to reference new project structure

- [x] Update `GraphlessDB.Tests` project references
- [x] Add explicit reference to GraphlessDB.Core
- [x] Add explicit reference to GraphlessDB.Storage
- [x] Add explicit reference to GraphlessDB.Domain
- [x] Add explicit reference to GraphlessDB.Query
- [x] Keep reference to GraphlessDB (main project)
- [x] Keep analyzer reference to GraphlessDB.Analyzers
- [x] Update `GraphlessDB.DynamoDB.Tests` project references (inherits from GraphlessDB.Tests)
- [x] Verify test utilities are accessible across all projects
- [x] Run `dotnet build` - ensure solution compiles
- [x] Run `dotnet test` - ensure all tests pass
- [x] Verify test coverage maintained (all 3,012 tests still passing)
- [x] Commit: "Stage 10: Update test projects"

**Validation Checkpoint**: ✅ Solution compiles (0 errors, 0 warnings), all 3,012 tests pass

### Stage 11: Update Solution File and Build Configuration
**Goal**: Update solution file to include all new projects

- [x] Add GraphlessDB.Core to solution
- [x] Add GraphlessDB.Storage to solution
- [x] Add GraphlessDB.Storage.InMemory to solution
- [x] Add GraphlessDB.Storage.FileBased to solution
- [x] Add GraphlessDB.Domain to solution
- [x] Add GraphlessDB.Query to solution
- [x] Verify all 11 projects are in solution (Analyzers, Core, Domain, DynamoDB, DynamoDB.Tests, Query, Storage, Storage.FileBased, Storage.InMemory, Tests, GraphlessDB)
- [x] Verify build configuration for all projects
- [x] Run clean rebuild at solution level
- [x] Verify `dotnet build src/GraphlessDB.sln` works
- [x] Verify `dotnet test src/GraphlessDB.sln` works
- [x] Commit: "Stage 11: Update solution configuration"

**Validation Checkpoint**: ✅ Solution builds cleanly, all 3,012 tests pass

### Stage 12: Documentation and Cleanup
**Goal**: Update documentation and clean up obsolete code

- [x] Update this architecture refactoring proposal to mark all stages complete
- [x] Update project structure documentation to reflect 11 projects
- [x] Update status to "Implementation Complete - Documentation in Progress"
- [x] Verify all old directories have been cleaned up
- [x] Verify solution structure is clean
- [ ] Update README.md with new architecture overview
- [ ] Update package descriptions for all projects
- [ ] Update XML documentation comments if needed
- [ ] Create architecture diagram (optional)
- [ ] Update CHANGELOG.md
- [ ] Run `dotnet build` - final verification
- [ ] Run `dotnet test` - final verification
- [ ] Commit: "Stage 12: Update documentation and cleanup"

**Final Validation**: In progress - documentation being updated

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
