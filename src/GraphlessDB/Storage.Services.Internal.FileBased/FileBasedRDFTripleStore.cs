/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Threading;
using Microsoft.Extensions.Options;

namespace GraphlessDB.Storage.Services.Internal.FileBased
{
    internal sealed class FileBasedRDFTripleStore : IRDFTripleStore<StoreType.Data>
    {
        private readonly string _storagePath;
        private readonly int _partitionCount;
        private readonly Lock _locker;
        private readonly IFileBasedRDFEventReader _rdfEventHandler;

        public FileBasedRDFTripleStore(
            IOptions<GraphOptions> graphOptions,
            IOptions<FileBasedRDFTripleStoreOptions> storageOptions,
            IFileBasedRDFEventReader rdfEventHandler)
        {
            _locker = new Lock();
            _partitionCount = graphOptions.Value.PartitionCount;
            _storagePath = storageOptions.Value.StoragePath;
            _rdfEventHandler = rdfEventHandler ?? throw new ArgumentNullException(nameof(rdfEventHandler));

            InitializeStorageDirectory();
        }

        public Task<GetRDFTriplesResponse> GetRDFTriplesAsync(
            GetRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            lock (_locker)
            {
                return Task.FromResult(
                    new GetRDFTriplesResponse(request
                        .Keys
                        .Select(key => GetRDFTriple(request.TableName, key))
                        .ToImmutableList(),
                        RDFTripleStoreConsumedCapacity.None()));
            }
        }

        public Task<ScanRDFTriplesResponse> ScanRDFTriplesAsync(
            ScanRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            lock (_locker)
            {
                var allTriples = new List<RDFTriple>();
                
                for (int i = 0; i < _partitionCount; i++)
                {
                    var partitionTriples = LoadPartitionData(i);
                    allTriples.AddRange(partitionTriples);
                    
                    if (allTriples.Count >= request.Limit)
                    {
                        break;
                    }
                }

                var hasNextPage = allTriples.Count > request.Limit;
                var items = allTriples.Take(request.Limit).ToImmutableList();
                
                return Task.FromResult(new ScanRDFTriplesResponse(
                    items,
                    hasNextPage,
                    RDFTripleStoreConsumedCapacity.None()));
            }
        }

        public Task<WriteRDFTriplesResponse> WriteRDFTriplesAsync(
            WriteRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            lock (_locker)
            {
                request.Items.ForEach(WriteRDFTriple);
            }

            request.Items.ForEach(OnWriteRDFTripleEvent);
            return Task.FromResult(new WriteRDFTriplesResponse(RDFTripleStoreConsumedCapacity.None()));
        }

        public Task<QueryRDFTriplesResponse> QueryRDFTriplesAsync(
            QueryRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            lock (_locker)
            {
                var partitionIndex = GetPartitionIndex(request.Subject);
                var partition = LoadPartitionData(partitionIndex);
                
                var subjectTriples = partition
                    .Where(t => t.Subject == request.Subject)
                    .OrderBy(t => t.Predicate)
                    .ToList();

                if (subjectTriples.Count == 0)
                {
                    return Task.FromResult(new QueryRDFTriplesResponse([], false, RDFTripleStoreConsumedCapacity.None()));
                }

                var filteredTriples = subjectTriples
                    .Where(t => t.Predicate.StartsWith(request.PredicateBeginsWith, StringComparison.Ordinal))
                    .ToList();

                if (request.ExclusiveStartKey != null)
                {
                    var startIndex = filteredTriples.FindIndex(t => 
                        t.Subject == request.ExclusiveStartKey.Subject && 
                        t.Predicate == request.ExclusiveStartKey.Predicate);
                    
                    if (startIndex >= 0)
                    {
                        filteredTriples = request.ScanIndexForward 
                            ? filteredTriples.Skip(startIndex + 1).ToList()
                            : filteredTriples.Take(startIndex).Reverse().ToList();
                    }
                }

                if (!request.ScanIndexForward)
                {
                    filteredTriples.Reverse();
                }

                var hasNextPage = filteredTriples.Count > request.Limit;
                var items = filteredTriples.Take(request.Limit).ToImmutableList();

                return Task.FromResult(new QueryRDFTriplesResponse(items, hasNextPage, RDFTripleStoreConsumedCapacity.None()));
            }
        }

        public Task<QueryRDFTriplesResponse> QueryRDFTriplesByPartitionAndPredicateAsync(
            QueryRDFTriplesByPartitionAndPredicateRequest request, CancellationToken cancellationToken)
        {
            if (request.ExclusiveStartKey != null && request.ExclusiveStartKey.Partition != request.Partition)
            {
                throw new ArgumentException("ExclusiveStartKey partition and request partition are not the same");
            }

            lock (_locker)
            {
                var index = LoadPredicateIndex(GetPartitionIndex(request.Partition));
                
                var matchingEntries = index
                    .Where(e => e.Partition == request.Partition)
                    .Where(e => e.Predicate.StartsWith(request.PredicateBeginsWith, StringComparison.Ordinal))
                    .OrderBy(e => e.Predicate)
                    .ThenBy(e => e.Subject)
                    .ToList();

                if (request.ExclusiveStartKey != null)
                {
                    var startIndex = matchingEntries.FindIndex(e => 
                        e.Subject == request.ExclusiveStartKey.Subject && 
                        e.Predicate == request.ExclusiveStartKey.Predicate);
                    
                    if (startIndex >= 0)
                    {
                        matchingEntries = request.ScanIndexForward 
                            ? matchingEntries.Skip(startIndex + 1).ToList()
                            : matchingEntries.Take(startIndex).Reverse().ToList();
                    }
                }

                if (!request.ScanIndexForward)
                {
                    matchingEntries.Reverse();
                }

                var items = ImmutableList<RDFTriple>.Empty;
                foreach (var entry in matchingEntries.Take(request.Limit))
                {
                    var triple = GetRDFTriple(request.TableName, new RDFTripleKey(entry.Subject, entry.Predicate));
                    if (triple != null)
                    {
                        items = items.Add(triple);
                    }
                }

                var hasNextPage = matchingEntries.Count > request.Limit;
                return Task.FromResult(new QueryRDFTriplesResponse(items, hasNextPage, RDFTripleStoreConsumedCapacity.None()));
            }
        }

        public Task RunHouseKeepingAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        private void InitializeStorageDirectory()
        {
            var dataDir = Path.Combine(_storagePath, "data");
            var indexDir = Path.Combine(_storagePath, "indexes", "by-predicate");

            Directory.CreateDirectory(dataDir);
            Directory.CreateDirectory(indexDir);

            for (int i = 0; i < _partitionCount; i++)
            {
                var dataFile = GetDataFilePath(i);
                var indexFile = GetPredicateIndexFilePath(i);
                
                if (!File.Exists(dataFile))
                {
                    File.WriteAllText(dataFile, string.Empty);
                }
                
                if (!File.Exists(indexFile))
                {
                    File.WriteAllText(indexFile, string.Empty);
                }
            }
        }

        private string GetDataFilePath(int partitionIndex)
        {
            return Path.Combine(_storagePath, "data", $"partition-{partitionIndex}.jsonl");
        }

        private string GetPredicateIndexFilePath(int partitionIndex)
        {
            return Path.Combine(_storagePath, "indexes", "by-predicate", $"partition-{partitionIndex}.jsonl");
        }

        [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "RDFTriple is preserved")]
        private List<RDFTriple> LoadPartitionData(int partitionIndex)
        {
            var filePath = GetDataFilePath(partitionIndex);
            if (!File.Exists(filePath))
            {
                return new List<RDFTriple>();
            }

            var triples = new List<RDFTriple>();
            var lines = File.ReadAllLines(filePath);
            
            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                try
                {
                    var triple = JsonSerializer.Deserialize<RDFTriple>(line);
                    if (triple != null)
                    {
                        triples.Add(triple);
                    }
                }
                catch (JsonException)
                {
                }
            }

            return triples;
        }

        [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "PredicateIndexEntry is preserved")]
        private List<PredicateIndexEntry> LoadPredicateIndex(int partitionIndex)
        {
            var filePath = GetPredicateIndexFilePath(partitionIndex);
            if (!File.Exists(filePath))
            {
                return new List<PredicateIndexEntry>();
            }

            var entries = new List<PredicateIndexEntry>();
            var lines = File.ReadAllLines(filePath);
            
            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                try
                {
                    var entry = JsonSerializer.Deserialize<PredicateIndexEntry>(line);
                    if (entry != null)
                    {
                        entries.Add(entry);
                    }
                }
                catch (JsonException)
                {
                }
            }

            return entries;
        }

        [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "RDFTriple is preserved")]
        private void SavePartitionData(int partitionIndex, List<RDFTriple> triples)
        {
            var filePath = GetDataFilePath(partitionIndex);
            var sortedTriples = triples
                .OrderBy(t => t.Partition)
                .ThenBy(t => t.Subject)
                .ThenBy(t => t.Predicate)
                .ThenBy(t => t.IndexedObject)
                .ToList();

            using var writer = new StreamWriter(filePath);
            foreach (var triple in sortedTriples)
            {
                var json = JsonSerializer.Serialize(triple);
                writer.WriteLine(json);
            }
        }

        [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "PredicateIndexEntry is preserved")]
        private void SavePredicateIndex(int partitionIndex, List<PredicateIndexEntry> entries)
        {
            var filePath = GetPredicateIndexFilePath(partitionIndex);
            var sortedEntries = entries
                .OrderBy(e => e.Partition)
                .ThenBy(e => e.Predicate)
                .ThenBy(e => e.Subject)
                .ThenBy(e => e.IndexedObject)
                .ToList();

            using var writer = new StreamWriter(filePath);
            foreach (var entry in sortedEntries)
            {
                var json = JsonSerializer.Serialize(entry);
                writer.WriteLine(json);
            }
        }

        private RDFTriple? GetRDFTriple(string tableName, RDFTripleKey key)
        {
            var partitionIndex = GetPartitionIndex(key.Subject);
            var triples = LoadPartitionData(partitionIndex);
            return triples.FirstOrDefault(t => t.Subject == key.Subject && t.Predicate == key.Predicate);
        }

        private static int GetPartitionIndex(string subject)
        {
            if (string.IsNullOrWhiteSpace(subject))
            {
                throw new ArgumentException($"'{nameof(subject)}' cannot be null or whitespace.", nameof(subject));
            }

            return 0;
        }

        private void OnWriteRDFTripleEvent(WriteRDFTriple value)
        {
            if (value.Add != null)
            {
                _rdfEventHandler.OnRDFTripleAdded(value.Add.Item);
                return;
            }

            if (value.Update != null)
            {
                _rdfEventHandler.OnRDFTripleUpdated(value.Update.Item);
                return;
            }
        }

        private void WriteRDFTriple(WriteRDFTriple request)
        {
            if (request.Add != null)
            {
                AddRDFTriple(request.Add);
                return;
            }

            if (request.Update != null)
            {
                UpdateRDFTriple(request.Update);
                return;
            }

            if (request.Delete != null)
            {
                DeleteRDFTriple(request.Delete);
                return;
            }

            if (request.UpdateAllEdgesVersion != null)
            {
                UpdateAllEdgesVersion(request.UpdateAllEdgesVersion);
                return;
            }

            if (request.IncrementAllEdgesVersion != null)
            {
                IncrementAllEdgesVersion(request.IncrementAllEdgesVersion);
                return;
            }

            if (request.CheckRDFTripleVersion != null)
            {
                CheckRDFTripleVersion(request.CheckRDFTripleVersion);
                return;
            }

            throw new NotSupportedException();
        }

        private void AddRDFTriple(AddRDFTriple request)
        {
            var partitionIndex = GetPartitionIndex(request.Item.Subject);
            var triples = LoadPartitionData(partitionIndex);
            
            if (triples.Any(t => t.Subject == request.Item.Subject && t.Predicate == request.Item.Predicate))
            {
                throw new GraphlessDBOperationException("Item already exists");
            }

            triples.Add(request.Item);
            SavePartitionData(partitionIndex, triples);

            var index = LoadPredicateIndex(partitionIndex);
            index.Add(new PredicateIndexEntry(
                request.Item.Predicate,
                request.Item.Subject,
                request.Item.IndexedObject,
                request.Item.Partition));
            SavePredicateIndex(partitionIndex, index);
        }

        private void UpdateRDFTriple(UpdateRDFTriple request)
        {
            var partitionIndex = GetPartitionIndex(request.Item.Subject);
            var triples = LoadPartitionData(partitionIndex);
            
            var existingIndex = triples.FindIndex(t => 
                t.Subject == request.Item.Subject && t.Predicate == request.Item.Predicate);
            
            if (existingIndex < 0)
            {
                throw new GraphlessDBOperationException("Item does not exist");
            }

            var existing = triples[existingIndex];

            if (request.VersionDetailCondition.NodeVersion.HasValue && 
                (existing.VersionDetail == null || request.VersionDetailCondition.NodeVersion.Value != existing.VersionDetail.NodeVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            if (request.VersionDetailCondition.AllEdgesVersion.HasValue && 
                (existing.VersionDetail == null || request.VersionDetailCondition.AllEdgesVersion.Value != existing.VersionDetail.AllEdgesVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            triples[existingIndex] = request.Item;
            SavePartitionData(partitionIndex, triples);
        }

        private void DeleteRDFTriple(DeleteRDFTriple request)
        {
            var partitionIndex = GetPartitionIndex(request.Key.Subject);
            var triples = LoadPartitionData(partitionIndex);
            
            var existingIndex = triples.FindIndex(t => 
                t.Subject == request.Key.Subject && t.Predicate == request.Key.Predicate);
            
            if (existingIndex < 0)
            {
                throw new GraphlessDBOperationException("Item does not exist");
            }

            var existing = triples[existingIndex];

            if (request.VersionDetailCondition.NodeVersion.HasValue && 
                (existing.VersionDetail == null || request.VersionDetailCondition.NodeVersion.Value != existing.VersionDetail.NodeVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            if (request.VersionDetailCondition.AllEdgesVersion.HasValue && 
                (existing.VersionDetail == null || request.VersionDetailCondition.AllEdgesVersion.Value != existing.VersionDetail.AllEdgesVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            triples.RemoveAt(existingIndex);
            SavePartitionData(partitionIndex, triples);

            var index = LoadPredicateIndex(partitionIndex);
            index.RemoveAll(e => e.Subject == request.Key.Subject && e.Predicate == request.Key.Predicate);
            SavePredicateIndex(partitionIndex, index);
        }

        private void UpdateAllEdgesVersion(UpdateRDFTripleAllEdgesVersion request)
        {
            var partitionIndex = GetPartitionIndex(request.Key.Subject);
            var triples = LoadPartitionData(partitionIndex);
            
            var existingIndex = triples.FindIndex(t => 
                t.Subject == request.Key.Subject && t.Predicate == request.Key.Predicate);
            
            if (existingIndex < 0)
            {
                throw new GraphlessDBOperationException("Item does not exist");
            }

            var existing = triples[existingIndex];

            if (request.VersionDetailCondition.NodeVersion.HasValue && 
                (existing.VersionDetail == null || request.VersionDetailCondition.NodeVersion.Value != existing.VersionDetail.NodeVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            if (request.VersionDetailCondition.AllEdgesVersion.HasValue && 
                (existing.VersionDetail == null || request.VersionDetailCondition.AllEdgesVersion.Value != existing.VersionDetail.AllEdgesVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            if (existing.VersionDetail == null)
            {
                throw new GraphlessDBOperationException("VersionDetail was not expected to be null");
            }

            triples[existingIndex] = new RDFTriple(
                existing.Subject,
                existing.Predicate,
                existing.IndexedObject,
                existing.Object,
                existing.Partition,
                existing.VersionDetail with
                {
                    AllEdgesVersion = request.AllEdgesVersion
                });
            
            SavePartitionData(partitionIndex, triples);
        }

        private void IncrementAllEdgesVersion(IncrementRDFTripleAllEdgesVersion request)
        {
            var partitionIndex = GetPartitionIndex(request.Key.Subject);
            var triples = LoadPartitionData(partitionIndex);
            
            var existingIndex = triples.FindIndex(t => 
                t.Subject == request.Key.Subject && t.Predicate == request.Key.Predicate);
            
            if (existingIndex < 0)
            {
                throw new GraphlessDBOperationException("Item does not exist");
            }

            var existing = triples[existingIndex];

            if (existing.VersionDetail == null)
            {
                throw new GraphlessDBOperationException("VersionDetail was not expected to be null");
            }

            triples[existingIndex] = new RDFTriple(
                existing.Subject,
                existing.Predicate,
                existing.IndexedObject,
                existing.Object,
                existing.Partition,
                existing.VersionDetail with
                {
                    AllEdgesVersion = existing.VersionDetail.AllEdgesVersion + 1
                });
            
            SavePartitionData(partitionIndex, triples);
        }

        private void CheckRDFTripleVersion(CheckRDFTripleVersion request)
        {
            var partitionIndex = GetPartitionIndex(request.Key.Subject);
            var triples = LoadPartitionData(partitionIndex);
            
            var existing = triples.FirstOrDefault(t => 
                t.Subject == request.Key.Subject && t.Predicate == request.Key.Predicate);
            
            if (existing == null)
            {
                throw new GraphlessDBOperationException("Item does not exist");
            }

            if (request.VersionDetailCondition.NodeVersion.HasValue && 
                (existing.VersionDetail == null || request.VersionDetailCondition.NodeVersion.Value != existing.VersionDetail.NodeVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            if (request.VersionDetailCondition.AllEdgesVersion.HasValue && 
                (existing.VersionDetail == null || request.VersionDetailCondition.AllEdgesVersion.Value != existing.VersionDetail.AllEdgesVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }
        }

        private sealed record PredicateIndexEntry(
            string Predicate,
            string Subject,
            string IndexedObject,
            string Partition);
    }
}
