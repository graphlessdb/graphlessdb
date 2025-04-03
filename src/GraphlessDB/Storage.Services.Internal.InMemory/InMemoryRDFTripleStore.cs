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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;
using Microsoft.Extensions.Options;

namespace GraphlessDB.Storage.Services.Internal.InMemory
{
    internal sealed class InMemoryRDFTripleStore : IRDFTripleStore<StoreType.Data>
    {
        private readonly Dictionary<string, InMemoryRDFTripleStoreTable> _tables;
        private readonly Dictionary<string, InMemoryRDFTripleStoreIndexTable> _indexes;
        private readonly Lock _locker;
        private readonly IInMemoryRDFEventReader _rdfEventHandler;

        public InMemoryRDFTripleStore(IOptions<GraphOptions> graphOptions, IInMemoryRDFEventReader rdfEventHandler)
        {
            _locker = new Lock();

            _tables = new Dictionary<string, InMemoryRDFTripleStoreTable>
            {
                { graphOptions.Value.TableName, InMemoryRDFTripleStoreTable.Create(graphOptions.Value.TableName, graphOptions.Value.PartitionCount) },
            };

            _indexes = new Dictionary<string, InMemoryRDFTripleStoreIndexTable>
            {
                {
                    GetByPredicateIndexName(graphOptions.Value.TableName),
                    InMemoryRDFTripleStoreIndexTable.Create(GetByPredicateIndexName(graphOptions.Value.TableName), graphOptions.Value.PartitionCount)
                },
                {
                    GetByIndexedObjectIndexName(graphOptions.Value.TableName),
                    InMemoryRDFTripleStoreIndexTable.Create(GetByIndexedObjectIndexName(graphOptions.Value.TableName), graphOptions.Value.PartitionCount)
                },
            };
            _rdfEventHandler = rdfEventHandler ?? throw new ArgumentNullException(nameof(rdfEventHandler));
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
                // TODO Single partition limitation
                var partition = _tables[request.TableName].Partitions.Single();

                if (request.ExclusiveStartKey != null)
                {
                    throw new NotSupportedException();
                }

                var hasNextPage = partition.ItemsByKey.Count <= request.Limit;
                return Task.FromResult(new ScanRDFTriplesResponse(
                    partition.ItemsByKey.Values.Take(request.Limit).ToImmutableList(),
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

        public Task<QueryRDFTriplesResponse> QueryRDFTriplesAsync(
            QueryRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            lock (_locker)
            {
                var tablePartition = _tables[request.TableName].Partitions[GetPartitionIndex(request.Subject)];
                if (!tablePartition.PredicatesBySubject.SortKeysByPartitionKey.TryGetValue(request.Subject, out var predicates))
                {
                    return Task.FromResult(new QueryRDFTriplesResponse([], false, RDFTripleStoreConsumedCapacity.None()));
                }

                var exclusiveStartIndex = request.ExclusiveStartKey != null
                    ? GetExclusiveStartIndex(request.ExclusiveStartKey, request.ScanIndexForward, predicates)
                    : null;

                var sortKeyIndex = GetSortKeyStartIndexInclusive(
                    predicates, exclusiveStartIndex, request.ScanIndexForward, request.PredicateBeginsWith);

                var items = ImmutableList<RDFTriple>.Empty;
                while (request.ScanIndexForward ? sortKeyIndex < predicates.Count : sortKeyIndex > 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var predicate = predicates[sortKeyIndex];
                    if (!predicate.StartsWith(request.PredicateBeginsWith, StringComparison.Ordinal))
                    {
                        break;
                    }

                    if (items.Count >= request.Limit)
                    {
                        return Task.FromResult(new QueryRDFTriplesResponse(items, true, RDFTripleStoreConsumedCapacity.None()));
                    }

                    var key = new RDFTripleKey(request.Subject, predicate);
                    var item = tablePartition.ItemsByKey[key];
                    items = items.Add(item);
                    sortKeyIndex = request.ScanIndexForward ? sortKeyIndex + 1 : sortKeyIndex - 1;
                }

                return Task.FromResult(new QueryRDFTriplesResponse(items, false, RDFTripleStoreConsumedCapacity.None()));
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
                var indexPartition = _indexes[GetByPredicateIndexName(request.TableName)].Partitions[GetPartitionIndex(request.Partition)];
                if (!indexPartition.SortKeysByPartitionKey.TryGetValue(request.Partition, out var predicates))
                {
                    return Task.FromResult(new QueryRDFTriplesResponse([], false, RDFTripleStoreConsumedCapacity.None()));
                }

                var rdfTripleKeys = indexPartition.RDFTripleKeysByPartitionKey[request.Partition];

                var exclusiveStartIndex = request.ExclusiveStartKey != null
                    ? GetExclusiveStartIndex(request.ExclusiveStartKey, request.ScanIndexForward, predicates, rdfTripleKeys)
                    : null;

                var sortKeyIndex = GetSortKeyStartIndexInclusive(
                    predicates, exclusiveStartIndex, request.ScanIndexForward, request.PredicateBeginsWith);

                var items = ImmutableList<RDFTriple>.Empty;
                while (request.ScanIndexForward ? sortKeyIndex < predicates.Count : sortKeyIndex > 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var predicate = predicates[sortKeyIndex];
                    if (!predicate.StartsWith(request.PredicateBeginsWith, StringComparison.Ordinal))
                    {
                        break;
                    }

                    if (items.Count >= request.Limit)
                    {
                        return Task.FromResult(new QueryRDFTriplesResponse(items, true, RDFTripleStoreConsumedCapacity.None()));
                    }

                    var table = _tables[request.TableName];
                    var rdfTripleKey = rdfTripleKeys[sortKeyIndex];
                    var tablePartition = table.Partitions[GetPartitionIndex(rdfTripleKey.Subject)];
                    var item = tablePartition.ItemsByKey[rdfTripleKey];
                    items = items.Add(item);
                    sortKeyIndex = request.ScanIndexForward ? sortKeyIndex + 1 : sortKeyIndex - 1;
                }

                return Task.FromResult(new QueryRDFTriplesResponse(items, false, RDFTripleStoreConsumedCapacity.None()));
            }
        }

        public Task RunHouseKeepingAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        private static int GetSortKeyStartIndexInclusive(
            List<string> predicates, int? exclusiveStartIndex, bool scanIndexForward, string predicateBeginsWith)
        {
            return scanIndexForward
                ? GetSortKeyScanningForwards(predicates, exclusiveStartIndex, predicateBeginsWith)
                : GetSortKeyScanningBackwards(predicates, exclusiveStartIndex, predicateBeginsWith);
        }

        private static int GetSortKeyScanningBackwards(List<string> predicates, int? exclusiveStartIndex, string predicateBeginsWith)
        {
            return exclusiveStartIndex.HasValue
                ? GetSortKeyScanningBackwardsWithExclusiveStartKey(predicates, exclusiveStartIndex.Value, predicateBeginsWith)
                : GetSortKeyScanningBackwardsWithoutExclusiveStartKey(predicates, predicateBeginsWith);
        }

        private static int GetSortKeyScanningForwards(List<string> predicates, int? exclusiveStartIndex, string predicateBeginsWith)
        {
            var fwdInclusiveStartIndex = exclusiveStartIndex == null ? 0 : exclusiveStartIndex.Value + 1;
            var fwdCount = predicates.Count - fwdInclusiveStartIndex;
            var fwdIndex = predicates.BinarySearch(fwdInclusiveStartIndex, fwdCount, predicateBeginsWith, StringComparer.Ordinal);
            if (fwdIndex < 0)
            {
                fwdIndex = ~fwdIndex;
            }

            return fwdIndex;
        }

        private static int GetSortKeyScanningBackwardsWithExclusiveStartKey(List<string> predicates, int exclusiveStartIndex, string predicateBeginsWith)
        {
            var revIndex = predicates.BinarySearch(0, exclusiveStartIndex - 1, predicateBeginsWith, StringComparer.Ordinal);
            if (revIndex < 0)
            {
                revIndex = ~revIndex;
            }

            while (revIndex < exclusiveStartIndex - 1 && predicates[revIndex + 1].StartsWith(predicateBeginsWith, StringComparison.Ordinal))
            {
                revIndex++;
            }

            return revIndex;
        }

        private static int GetSortKeyScanningBackwardsWithoutExclusiveStartKey(List<string> predicates, string predicateBeginsWith)
        {
            var revIndex = predicates.BinarySearch(0, predicates.Count, predicateBeginsWith, StringComparer.Ordinal);
            if (revIndex < 0)
            {
                revIndex = ~revIndex;
            }

            while (revIndex < predicates.Count - 1 && predicates[revIndex + 1].StartsWith(predicateBeginsWith, StringComparison.Ordinal))
            {
                revIndex++;
            }

            return revIndex;
        }

        private static int? GetExclusiveStartIndex(
            RDFTripleKey exclusiveStartKey, bool scanIndexForward, List<string> predicates)
        {
            var exclusiveStartIndex = predicates.BinarySearch(exclusiveStartKey.Predicate, StringComparer.Ordinal);
            if (exclusiveStartIndex < 0)
            {
                throw new GraphlessDBOperationException("Exclusive start could not be found");
            }

            if (scanIndexForward)
            {
                // Make sure we walk backwards to the beginning of the duplicate predicates
                while (exclusiveStartIndex > 0 && predicates[exclusiveStartIndex - 1] == exclusiveStartKey.Predicate)
                {
                    exclusiveStartIndex--;
                }
            }
            else
            {
                // Make sure we walk forwards to the beginning of the duplicate predicates
                while (exclusiveStartIndex < predicates.Count - 1 && predicates[exclusiveStartIndex + 1] == exclusiveStartKey.Predicate)
                {
                    exclusiveStartIndex++;
                }
            }

            // while (rdfTripleKeys[exclusiveStartIndex].Subject != exclusiveStartKey.Subject)
            // {
            //     exclusiveStartIndex = scanIndexForward ? exclusiveStartIndex + 1 : exclusiveStartIndex - 1;

            //     if (rdfTripleKeys[exclusiveStartIndex].Predicate != exclusiveStartKey.Predicate)
            //     {
            //         throw new InvalidOperationException("Exclusive start could not be found");
            //     }
            // }

            // exclusiveStartIndex = scanIndexForward ? exclusiveStartIndex + 1 : exclusiveStartIndex - 1;

            return exclusiveStartIndex;
        }

        private static int? GetExclusiveStartIndex(
            RDFTripleKeyWithPartition exclusiveStartKey, bool scanIndexForward, List<string> predicates, List<RDFTripleKey> rdfTripleKeys)
        {
            var exclusiveStartIndex = predicates.BinarySearch(exclusiveStartKey.Predicate, StringComparer.Ordinal);
            if (exclusiveStartIndex < 0)
            {
                throw new GraphlessDBOperationException("Exclusive start could not be found");
            }

            if (scanIndexForward)
            {
                // Make sure we walk backwards to the beginning of the duplicate predicates
                while (exclusiveStartIndex > 0 && predicates[exclusiveStartIndex - 1] == exclusiveStartKey.Predicate)
                {
                    exclusiveStartIndex--;
                }
            }
            else
            {
                // Make sure we walk forwards to the beginning of the duplicate predicates
                while (exclusiveStartIndex < predicates.Count - 1 && predicates[exclusiveStartIndex + 1] == exclusiveStartKey.Predicate)
                {
                    exclusiveStartIndex++;
                }
            }

            while (rdfTripleKeys[exclusiveStartIndex].Subject != exclusiveStartKey.Subject)
            {
                exclusiveStartIndex = scanIndexForward ? exclusiveStartIndex + 1 : exclusiveStartIndex - 1;

                if (rdfTripleKeys[exclusiveStartIndex].Predicate != exclusiveStartKey.Predicate)
                {
                    throw new GraphlessDBOperationException("Exclusive start could not be found");
                }
            }

            // exclusiveStartIndex = scanIndexForward ? exclusiveStartIndex + 1 : exclusiveStartIndex - 1;

            return exclusiveStartIndex;
        }

        private static string GetByPredicateIndexName(string tableName)
        {
            return $"{tableName}ByPredicate";
        }

        private static string GetByIndexedObjectIndexName(string tableName)
        {
            return $"{tableName}ByIndexedObject";
        }

        private RDFTriple? GetRDFTriple(string tableName, RDFTripleKey key)
        {
            var partition = _tables[tableName].Partitions[GetPartitionIndex(key.Subject)];
            partition.ItemsByKey.TryGetValue(key, out var item);
            return item;
        }

        private static int GetPartitionIndex(string subject)
        {
            if (string.IsNullOrWhiteSpace(subject))
            {
                throw new ArgumentException($"'{nameof(subject)}' cannot be null or whitespace.", nameof(subject));
            }

            return 0;
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

        private void CheckRDFTripleVersion(CheckRDFTripleVersion request)
        {
            var partition = _tables[request.TableName].Partitions[GetPartitionIndex(request.Key.Subject)];
            var exists = partition.ItemsByKey.TryGetValue(request.Key, out var item);
            if (!exists)
            {
                throw new GraphlessDBOperationException("Item does not exist");
            }

            if (request.VersionDetailCondition.NodeVersion.HasValue && (item?.VersionDetail == null || request.VersionDetailCondition.NodeVersion.Value != item.VersionDetail.NodeVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            if (request.VersionDetailCondition.AllEdgesVersion.HasValue && (item?.VersionDetail == null || request.VersionDetailCondition.AllEdgesVersion.Value != item.VersionDetail.AllEdgesVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }
        }

        private void IncrementAllEdgesVersion(IncrementRDFTripleAllEdgesVersion request)
        {
            var partition = _tables[request.TableName].Partitions[GetPartitionIndex(request.Key.Subject)];
            var exists = partition.ItemsByKey.TryGetValue(request.Key, out var item);
            if (!exists || item == null)
            {
                throw new GraphlessDBOperationException("Item does not exist");
            }

            // TODO ???
            // if (request.VersionDetailCondition.NodeVersion.HasValue && (item?.VersionDetail == null || request.VersionDetailCondition.NodeVersion.Value != item.VersionDetail.NodeVersion))
            // {
            //     throw new GraphlessDBConcurrencyException("Version constraint not matched");
            // }

            // if (request.VersionDetailCondition.AllEdgesVersion.HasValue && (item?.VersionDetail == null || request.VersionDetailCondition.AllEdgesVersion.Value != item.VersionDetail.AllEdgesVersion))
            // {
            //     throw new GraphlessDBConcurrencyException("Version constraint not matched");
            // }

            if (item.VersionDetail == null)
            {
                throw new GraphlessDBOperationException("VersionDetail was not expected to be null");
            }

            partition.ItemsByKey[request.Key] = new RDFTriple(
                item.Subject,
                item.Predicate,
                item.IndexedObject,
                item.Object,
                item.Partition,
                item.VersionDetail with
                {
                    AllEdgesVersion = item.VersionDetail.AllEdgesVersion + 1
                });
        }

        private void UpdateAllEdgesVersion(UpdateRDFTripleAllEdgesVersion request)
        {
            var partition = _tables[request.TableName].Partitions[GetPartitionIndex(request.Key.Subject)];
            var exists = partition.ItemsByKey.TryGetValue(request.Key, out var item);
            if (!exists || item == null)
            {
                throw new GraphlessDBOperationException("Item does not exist");
            }

            if (request.VersionDetailCondition.NodeVersion.HasValue && (item?.VersionDetail == null || request.VersionDetailCondition.NodeVersion.Value != item.VersionDetail.NodeVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            if (request.VersionDetailCondition.AllEdgesVersion.HasValue && (item?.VersionDetail == null || request.VersionDetailCondition.AllEdgesVersion.Value != item.VersionDetail.AllEdgesVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            if (item.VersionDetail == null)
            {
                throw new GraphlessDBOperationException("VersionDetail was not expected to be null");
            }

            partition.ItemsByKey[request.Key] = new RDFTriple(
                item.Subject,
                item.Predicate,
                item.IndexedObject,
                item.Object,
                item.Partition,
                item.VersionDetail with
                {
                    AllEdgesVersion = request.AllEdgesVersion
                });
        }

        private void DeleteRDFTriple(DeleteRDFTriple request)
        {
            var rdfTriple = DeleteRDFTripleInTable(request);
            DeleteRDFTripleInByPredicateIndex(request, rdfTriple);
            DeleteRDFTripleInByIndexedObjectIndex(request, rdfTriple);
        }

        private RDFTriple DeleteRDFTripleInTable(DeleteRDFTriple request)
        {
            var partition = _tables[request.TableName].Partitions[GetPartitionIndex(request.Key.Subject)];
            var exists = partition.ItemsByKey.TryGetValue(request.Key, out var item);
            if (!exists || item == null)
            {
                throw new GraphlessDBOperationException("Item does not exist");
            }

            if (request.VersionDetailCondition.NodeVersion.HasValue && (item?.VersionDetail == null || request.VersionDetailCondition.NodeVersion.Value != item.VersionDetail.NodeVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            if (request.VersionDetailCondition.AllEdgesVersion.HasValue && (item?.VersionDetail == null || request.VersionDetailCondition.AllEdgesVersion.Value != item.VersionDetail.AllEdgesVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            if (!partition.ItemsByKey.Remove(request.Key))
            {
                throw new GraphlessDBOperationException();
            }

            if (!partition.PredicatesBySubject.SortKeysByPartitionKey[request.Key.Subject].Remove(request.Key.Predicate))
            {
                throw new GraphlessDBOperationException();
            }

            return item;
        }

        private void DeleteRDFTripleInByPredicateIndex(DeleteRDFTriple request, RDFTriple rdfTriple)
        {
            var partition = _indexes[GetByPredicateIndexName(request.TableName)].Partitions[GetPartitionIndex(rdfTriple.Partition)];
            var pos = partition.SortKeysByPartitionKey[rdfTriple.Partition].BinarySearch(rdfTriple.Predicate, StringComparer.Ordinal);
            if (pos < 0)
            {
                throw new GraphlessDBOperationException();
            }

            partition.SortKeysByPartitionKey[rdfTriple.Partition].RemoveAt(pos);

            partition.RDFTripleKeysByPartitionKey[rdfTriple.Partition].RemoveAt(pos);
        }

        private void DeleteRDFTripleInByIndexedObjectIndex(DeleteRDFTriple request, RDFTriple rdfTriple)
        {
            var partition = _indexes[GetByIndexedObjectIndexName(request.TableName)].Partitions[GetPartitionIndex(rdfTriple.Partition)];
            var pos = partition.SortKeysByPartitionKey[rdfTriple.Partition].BinarySearch(rdfTriple.IndexedObject, StringComparer.Ordinal);
            if (pos < 0)
            {
                throw new GraphlessDBOperationException();
            }

            partition.SortKeysByPartitionKey[rdfTriple.Partition].RemoveAt(pos);

            partition.RDFTripleKeysByPartitionKey[rdfTriple.Partition].RemoveAt(pos);
        }

        private void UpdateRDFTriple(UpdateRDFTriple request)
        {
            UpdateRDFTripleInTable(request);
            // TODO Update indexes ???
        }

        private void UpdateRDFTripleInTable(UpdateRDFTriple request)
        {
            var partition = _tables[request.TableName].Partitions[GetPartitionIndex(request.Item.Subject)];
            var exists = partition.ItemsByKey.TryGetValue(request.Item.AsKey(), out var item);
            if (!exists)
            {
                throw new GraphlessDBOperationException("Item does not exist");
            }

            if (request.VersionDetailCondition.NodeVersion.HasValue && (item?.VersionDetail == null || request.VersionDetailCondition.NodeVersion.Value != item.VersionDetail.NodeVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            if (request.VersionDetailCondition.AllEdgesVersion.HasValue && (item?.VersionDetail == null || request.VersionDetailCondition.AllEdgesVersion.Value != item.VersionDetail.AllEdgesVersion))
            {
                throw new GraphlessDBConcurrencyException("Version constraint not matched");
            }

            partition.ItemsByKey[request.Item.AsKey()] = request.Item;
        }

        private void AddRDFTriple(AddRDFTriple request)
        {
            AddRDFTripleToTable(request);
            AddRDFTripleToByPredicateIndex(request);
            AddRDFTripleToByIndexedObjectIndex(request);
        }

        private void AddRDFTripleToTable(AddRDFTriple request)
        {
            var partition = _tables[request.TableName].Partitions[GetPartitionIndex(request.Item.Subject)];
            if (partition.ItemsByKey.ContainsKey(request.Item.AsKey()))
            {
                throw new GraphlessDBOperationException("Item already exists");
            }

            partition.ItemsByKey[request.Item.AsKey()] = request.Item;
            if (!partition.PredicatesBySubject.SortKeysByPartitionKey.TryGetValue(request.Item.Subject, out var predicates))
            {
                predicates = [];
                partition.PredicatesBySubject.SortKeysByPartitionKey[request.Item.Subject] = predicates;
            }

            var pos = predicates.BinarySearch(request.Item.Predicate, StringComparer.Ordinal);
            if (pos >= 0)
            {
                throw new GraphlessDBOperationException("Key already exists");
            }

            pos = ~pos;
            predicates.Insert(pos, request.Item.Predicate);
        }

        private void AddRDFTripleToByPredicateIndex(AddRDFTriple request)
        {
            var partition = _indexes[GetByPredicateIndexName(request.TableName)].Partitions[GetPartitionIndex(request.Item.Partition)];
            if (!partition.SortKeysByPartitionKey.TryGetValue(request.Item.Partition, out var predicates))
            {
                predicates = [];
                partition.SortKeysByPartitionKey[request.Item.Partition] = predicates;
                partition.RDFTripleKeysByPartitionKey[request.Item.Partition] = [];
            }

            var pos = predicates.BinarySearch(request.Item.Predicate, StringComparer.Ordinal);
            if (pos < 0)
            {
                pos = ~pos;
            }

            predicates.Insert(pos, request.Item.Predicate);
            partition.RDFTripleKeysByPartitionKey[request.Item.Partition].Insert(pos, request.Item.AsKey());
        }

        private void AddRDFTripleToByIndexedObjectIndex(AddRDFTriple request)
        {
            var partition = _indexes[GetByIndexedObjectIndexName(request.TableName)].Partitions[GetPartitionIndex(request.Item.Partition)];
            if (!partition.SortKeysByPartitionKey.TryGetValue(request.Item.Partition, out var indexedObjects))
            {
                indexedObjects = [];
                partition.SortKeysByPartitionKey[request.Item.Partition] = indexedObjects;
                partition.RDFTripleKeysByPartitionKey[request.Item.Partition] = [];
            }

            var pos = indexedObjects.BinarySearch(request.Item.IndexedObject, StringComparer.Ordinal);
            if (pos < 0)
            {
                pos = ~pos;
            }

            indexedObjects.Insert(pos, request.Item.IndexedObject);
            partition.RDFTripleKeysByPartitionKey[request.Item.Partition].Insert(pos, request.Item.AsKey());
        }
    }
}
