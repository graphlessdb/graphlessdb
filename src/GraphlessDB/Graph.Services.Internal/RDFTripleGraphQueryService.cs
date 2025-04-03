/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;
using GraphlessDB.Linq;
using GraphlessDB.Logging;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services;
using Microsoft.Extensions.Logging;

namespace GraphlessDB.Graph.Services.Internal
{
    internal sealed class RDFTripleGraphQueryService(
        IGraphSettingsService graphSettingsService,
        IRDFTripleFactory rdfTripleFactory,
        IRDFTripleStore rdfTripleStore,
        IGraphCursorSerializationService cursorSerializer,
        IRDFTripleExclusiveStartKeyService exclusiveStartKeyService,
        ILogger<RDFTripleGraphQueryService> logger) : IGraphQueryService
    {
        public async Task ClearAsync(CancellationToken cancellationToken)
        {
            var settings = graphSettingsService.GetGraphSettings();
            await Task.WhenAll(Enumerable
                .Range(0, settings.PartitionCount)
                .Select(partition => ClearPartitionAsync(partition, settings, cancellationToken)));
        }

        private async Task ClearPartitionAsync(int partition, GraphSettings settings, CancellationToken cancellationToken)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var rdfRequest = new QueryRDFTriplesByPartitionAndPredicateRequest(
                    settings.TableName,
                    partition.ToString(CultureInfo.InvariantCulture),
                    settings.GraphName,
                    null,
                    true,
                    1000,
                    false,
                    true);

                var response = await rdfTripleStore.QueryRDFTriplesByPartitionAndPredicateAsync(rdfRequest, cancellationToken);
                if (response.Items.IsEmpty)
                {
                    return;
                }

                var mutId = new MutationId(Guid.NewGuid().ToString());
                var deleteItems = response
                    .Items
                    .Select(r => WriteRDFTriple.Create(new DeleteRDFTriple(settings.TableName, r.AsKey(), VersionDetailCondition.None)))
                    .ToImmutableList();

                var writeRequest = new WriteRDFTriplesRequest(mutId.Next(), true, deleteItems);
                await rdfTripleStore.WriteRDFTriplesAsync(writeRequest, cancellationToken);
                if (!response.HasNextPage)
                {
                    return;
                }
            }
        }

        public async Task<TryGetNodesResponse> TryGetNodesAsync(
            TryGetNodesRequest request,
            CancellationToken cancellationToken)
        {
            if (request.Ids.IsEmpty)
            {
                return new TryGetNodesResponse([]);
            }

            var rdfRequest = ToReadRDFTriplesRequest(
                request, graphSettingsService.GetGraphSettings());

            var rdfResponse = await rdfTripleStore
                .GetRDFTriplesAsync(rdfRequest, cancellationToken);

            return ToTryGetNodesResponse(rdfResponse);
        }

        private TryGetNodesResponse ToTryGetNodesResponse(GetRDFTriplesResponse response)
        {
            return new TryGetNodesResponse(response
                .Items
                .Select(rdfTriple => rdfTriple != null
                    ? new RelayEdge<INode>(cursorSerializer.Serialize(Cursor.Create(CursorNode.Empty with { HasType = new HasTypeCursor(rdfTriple.Subject, rdfTriple.Partition, []) })), rdfTripleFactory.GetNode(rdfTriple))
                    : null)
                .ToImmutableList());
        }

        private static GetRDFTriplesRequest ToReadRDFTriplesRequest(
            TryGetNodesRequest request, GraphSettings options)
        {
            return new GetRDFTriplesRequest(
                options.TableName,
                request
                    .Ids
                    .Select(id => new RDFTripleKey(id, new HasType(options.GraphName, GlobalId.Parse(id).TypeName, id).ToString()))
                    .ToImmutableList(),
                request.ConsistentRead);
        }

        public async Task<TryGetEdgesResponse> TryGetEdgesAsync(
            TryGetEdgesRequest request,
            CancellationToken cancellationToken)
        {
            if (request.Keys.IsEmpty)
            {
                return new TryGetEdgesResponse([]);
            }

            var options = graphSettingsService.GetGraphSettings();
            var rdfRequest = ToReadRDFTriplesRequest(request, options);
            var rdfResponse = await rdfTripleStore.GetRDFTriplesAsync(rdfRequest, cancellationToken);

            return new TryGetEdgesResponse(rdfResponse
                .Items
                .Select((rdfTriple, i) =>
                {
                    if (rdfTriple == null)
                    {
                        return null;
                    }

                    var edgeCursor = Cursor.Create(CursorNode.Empty with
                    {
                        HasInEdge = new HasInEdgeCursor(rdfRequest.Keys[i].Subject, request.Keys[i].TypeName, request.Keys[i].OutId)
                    });

                    return new RelayEdge<IEdge>(
                        cursorSerializer.Serialize(edgeCursor),
                        rdfTripleFactory.GetEdge(rdfTriple));
                })
                .ToImmutableList());
        }

        private static GetRDFTriplesRequest ToReadRDFTriplesRequest(
            TryGetEdgesRequest request, GraphSettings options)
        {
            return new GetRDFTriplesRequest(
                options.TableName,
                request
                    .Keys
                    .Select(key => new RDFTripleKey(
                        key.InId,
                        new HasInEdge(options.GraphName, GlobalId.Parse(key.InId).TypeName, key.TypeName, key.InId, key.OutId).ToString()))
                    .ToImmutableList(),
                request.ConsistentRead);
        }

        public async Task<TryGetVersionedNodesResponse> TryGetVersionedNodesAsync(
            TryGetVersionedNodesRequest request,
            CancellationToken cancellationToken)
        {
            if (request.Keys.IsEmpty)
            {
                return new TryGetVersionedNodesResponse([]);
            }

            var options = graphSettingsService.GetGraphSettings();

            var rdfRequest = new GetRDFTriplesRequest(
                options.TableName,
                request
                    .Keys
                    .Select(key => new RDFTripleKey(
                        key.Id,
                        new HasBlob(options.GraphName, GlobalId.Parse(key.Id).TypeName, key.Version).ToString()))
                    .ToImmutableList(),
                request.ConsistentRead);

            var rdfResponse = await rdfTripleStore
                .GetRDFTriplesAsync(rdfRequest, cancellationToken);

            var nodes = rdfResponse
                .Items
                .Select(rdfTriple => rdfTriple != null ? new RelayEdge<INode>(
                    cursorSerializer.Serialize(Cursor.Create(CursorNode.Empty with { HasType = new HasTypeCursor(rdfTriple.Subject, rdfTriple.Partition, []) })), // TODO Check
                    AsVersionedNode(rdfTriple)) : null)
                .ToImmutableList();

            return new TryGetVersionedNodesResponse(nodes);
        }

        private INode AsVersionedNode(RDFTriple value)
        {
            try
            {
                var predicate = HasBlob.Parse(value.Predicate);
                var versionDetail = new VersionDetail(predicate.Version, 0);
                var rdfTriple = new RDFTriple(value.Subject, value.Predicate, value.IndexedObject, value.Object, value.Partition, versionDetail);
                return rdfTripleFactory.GetNode(rdfTriple);
            }
            catch (Exception)
            {
                logger.CouldNotConvertRDFTripleToVersionedNode(value);
                throw;
            }
        }

        public async Task<GetConnectionResponse> GetConnectionByTypeAsync(
            GetConnectionByTypeRequest request,
            CancellationToken cancellationToken)
        {
            var options = graphSettingsService.GetGraphSettings();
            var scanIndexForward = GetScanIndexForward(request.ConnectionArguments);
            var limit = GetLimit(request.ConnectionArguments);
            var exclusiveStartKeyCursor = exclusiveStartKeyService
                .TryGetHasTypeCursor(request.ConnectionArguments);

            var queryCount = options.PartitionCount;
            var rdfResponseArray = await Task.WhenAll(
                Enumerable
                    .Range(0, queryCount)
                    .Select(async queryIndex =>
                    {
                        if (!IsRequestRequired(queryIndex, exclusiveStartKeyCursor))
                        {
                            return null;
                        }

                        var partition = queryIndex.ToString(CultureInfo.InvariantCulture);
                        var rdfRequest = new QueryRDFTriplesByPartitionAndPredicateRequest(
                            options.TableName,
                            partition,
                            HasType.ByType(options.GraphName, request.TypeName),
                            exclusiveStartKeyService.TryGetHasTypeExclusiveStartKey(request.ConnectionArguments, queryIndex, request.TypeName),
                            scanIndexForward,
                            limit,
                            true,
                            false);

                        return await rdfTripleStore.QueryRDFTriplesByPartitionAndPredicateAsync(rdfRequest, cancellationToken);
                    }));

            // TODO: If the request was meant to be consistent read but it used an index where that is
            // not guaranteed then we need to do an addition consistent read on the main table for each item
            // before proceeding

            var rdfResponses = rdfResponseArray.ToImmutableList();
            var orderedRdfTriples = rdfResponses
                .SelectMany((response, queryIndex) => response?.Items.Select(rdfTriple => new RDFTripleWithQueryIndex(queryIndex, rdfTriple)) ?? [])
                .OrderByDirection(v => v.RDFTriple.Predicate, !scanIndexForward, StringComparer.Ordinal)
                .ThenBy(v => v.QueryIndex) // NOTE If the predicates match then ensure ordering by partition
                .ToImmutableList();

            var allEdges = ToHasTypeRelayEdges(rdfResponses, orderedRdfTriples, exclusiveStartKeyCursor?.QueryCursors, rdfTripleFactory.GetNode);

            var edges = allEdges
                .Take(limit)
                .ToImmutableList();

            var usedLast = request.ConnectionArguments.Last.HasValue;
            var pageInfo = new PageInfo(
                !usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r?.HasNextPage ?? false)),
                usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r?.HasNextPage ?? false)),
                edges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                edges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty);

            return new GetConnectionResponse(
                new Connection<RelayEdge<INode>, INode>(edges, pageInfo));
        }

        private static bool IsRequestRequired(int requestIndex, HasTypeCursor? exclusiveStartKeyCursor)
        {
            // If this is using an exclusive start key but no cursor info for this particular partition was found
            // this means that we reached the end of this partition already so it doesnt need to be searched
            if (exclusiveStartKeyCursor == null)
            {
                return true;
            }

            var queryCursor = exclusiveStartKeyCursor.QueryCursors[requestIndex];
            return queryCursor.Position != PartitionPosition.Finish;
        }

        private static bool IsRequestRequired(int requestIndex, HasPropCursor? exclusiveStartKeyCursor)
        {
            // If this is using an exclusive start key but no cursor info for this particular partition was found
            // this means that we reached the end of this partition already so it doesnt need to be searched
            if (exclusiveStartKeyCursor == null)
            {
                return true;
            }

            var queryCursor = exclusiveStartKeyCursor.QueryCursors[requestIndex];
            return queryCursor.Position != PartitionPosition.Finish;
        }

        public async Task<GetConnectionResponse> GetConnectionByTypeAndPropertyNameAsync(
            GetConnectionByTypeAndPropertyNameRequest request,
            CancellationToken cancellationToken)
        {
            var options = graphSettingsService.GetGraphSettings();
            var scanIndexForward = GetScanIndexForward(request.ConnectionArguments, request.OrderDesc);
            var limit = GetLimit(request.ConnectionArguments);
            var exclusiveStartKeyCursor = exclusiveStartKeyService
                .TryGetHasPropCursor(request.ConnectionArguments);

            var queryCount = options.PartitionCount;
            var rdfResponseArray = await Task.WhenAll(
                Enumerable
                    .Range(0, queryCount)
                    .Select(async queryIndex =>
                    {
                        if (!IsRequestRequired(queryIndex, exclusiveStartKeyCursor))
                        {
                            return null;
                        }

                        var partition = queryIndex.ToString(CultureInfo.InvariantCulture);
                        var rdfRequest = new QueryRDFTriplesByPartitionAndPredicateRequest(
                           options.TableName,
                           partition,
                           HasProp.PropertiesByTypeAndPropertyName(options.GraphName, request.TypeName, request.PropertyName),
                           exclusiveStartKeyService.TryGetPropertiesByTypeAndPropertyNameExclusiveStartKey(request.ConnectionArguments, queryIndex, request.TypeName, request.PropertyName),
                           scanIndexForward,
                           limit,
                           true,
                           false);

                        return await rdfTripleStore.QueryRDFTriplesByPartitionAndPredicateAsync(rdfRequest, cancellationToken);
                    }));

            // TODO: If the request was meant to be consistent read but it used an index where that is
            // not guaranteed then we need to do an addition consistent read on the main table for each item
            // before proceeding

            var rdfResponses = rdfResponseArray.ToImmutableList();
            var orderedRdfTriples = rdfResponses
                .SelectMany((response, queryIndex) => response?.Items.Select(rdfTriple => new RDFTripleWithQueryIndex(queryIndex, rdfTriple)) ?? [])
                .OrderByDirection(v => v.RDFTriple.Predicate, !scanIndexForward, StringComparer.Ordinal)
                .ThenBy(v => v.QueryIndex) // NOTE If the predicates match then ensure ordering by partition
                .ToImmutableList();

            var allEdges = ToHasPropRelayEdges(rdfResponses, orderedRdfTriples, exclusiveStartKeyCursor?.QueryCursors, v => v.Subject);

            var edges = allEdges
                .Take(limit)
                .ToImmutableList();

            var getNodesRequest = new GetNodesRequest(
                edges.Select(e => e.Node).ToImmutableList(),
                request.ConsistentRead);

            var getNodesResponse = await this.GetNodesAsync(getNodesRequest, cancellationToken);
            var nodesById = getNodesResponse.Nodes.ToImmutableDictionary(k => k.Node.Id);
            var nodeEdges = getNodesResponse
                .Nodes
                .Select((node, i) => new RelayEdge<INode>(edges[i].Cursor, node.Node))
                .ToImmutableList();

            var usedLast = request.ConnectionArguments.Last.HasValue;
            var pageInfo = new PageInfo(
                !usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r?.HasNextPage ?? false)),
                usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r?.HasNextPage ?? false)),
                edges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                edges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty);

            return new GetConnectionResponse(
                new Connection<RelayEdge<INode>, INode>(nodeEdges, pageInfo));
        }

        private ImmutableList<RelayEdge<T>> ToHasPropRelayEdges<T>(
            ImmutableList<QueryRDFTriplesResponse?> rdfResponses,
            ImmutableList<RDFTripleWithQueryIndex> orderedRdfTriples,
            ImmutableList<HasPropCursorQueryCursor>? queryCursors,
            Func<RDFTriple, T> nodeSelector)
        {
            var queryCount = rdfResponses.Count;

            var dataByQueryIndex = Enumerable
                .Range(0, queryCount)
                .ToImmutableDictionary(k => k, v => rdfResponses[v]);

            queryCursors ??= Enumerable
                    .Range(0, queryCount)
                    .Select(i => new HasPropCursorQueryCursor(
                        i,
                        // If no data in the partition then we must already be at the end
                        rdfResponses[i]?.Items.Count > 0 ? PartitionPosition.Start : PartitionPosition.Finish,
                        null))
                    .ToImmutableList();

            var dataPositionByQueryIndex = Enumerable
                .Range(0, queryCount)
                .Select(i => 0)
                .ToImmutableList();

            var allEdges = ImmutableList<RelayEdge<T>>.Empty;
            for (var i = 0; i < orderedRdfTriples.Count; i++)
            {
                var rdfTripleWithQueryIndex = orderedRdfTriples[i];
                var rdfTriple = rdfTripleWithQueryIndex.RDFTriple;
                var queryIndex = rdfTripleWithQueryIndex.QueryIndex;

                queryCursors = queryCursors.SetItem(
                    queryIndex,
                    new HasPropCursorQueryCursor(
                        queryIndex,
                        PartitionPosition.Cursor,
                        new HasPropCursorPartitionCursor(rdfTriple.Subject, HasProp.Parse(rdfTriple.Predicate).PropertyValue, rdfTriple.Partition)));

                var cursor = cursorSerializer.Serialize(Cursor.Create(ToHasPropCursorItem(rdfTriple, queryCursors)));
                var edge = new RelayEdge<T>(cursor, nodeSelector(rdfTriple));
                allEdges = allEdges.Add(edge);

                // Update partition cursors
                var countInQuery = dataByQueryIndex[queryIndex]?.Items.Count ?? 0;
                var newIndexInQuery = dataPositionByQueryIndex[queryIndex] + 1;
                dataPositionByQueryIndex = dataPositionByQueryIndex.SetItem(queryIndex, newIndexInQuery);

                var queryHasNextPage = dataByQueryIndex[queryIndex]?.HasNextPage ?? false;
                if (newIndexInQuery >= countInQuery && !queryHasNextPage)
                {
                    queryCursors = queryCursors.SetItem(
                        queryIndex,
                        new HasPropCursorQueryCursor(queryIndex, PartitionPosition.Finish, null));
                }
            }

            return allEdges;
        }

        private ImmutableList<RelayEdge<T>> ToHasTypeRelayEdges<T>(
            ImmutableList<QueryRDFTriplesResponse?> rdfResponses,
            ImmutableList<RDFTripleWithQueryIndex> orderedRdfTriples,
            ImmutableList<HasTypeCursorQueryCursor>? queryCursors,
            Func<RDFTriple, T> nodeSelector)
        {
            var queryCount = rdfResponses.Count;

            var dataByQueryIndex = Enumerable
                .Range(0, queryCount)
                .ToImmutableDictionary(k => k, v => rdfResponses[v]);

            queryCursors ??= Enumerable
                    .Range(0, queryCount)
                    .Select(i => new HasTypeCursorQueryCursor(
                        i,
                        // If no data in the partition then we must already be at the end
                        rdfResponses[i]?.Items.Count > 0 ? PartitionPosition.Start : PartitionPosition.Finish,
                        null))
                    .ToImmutableList();

            var dataPositionByQueryIndex = Enumerable
                .Range(0, queryCount)
                .Select(i => 0)
                .ToImmutableList();

            var allEdges = ImmutableList<RelayEdge<T>>.Empty;
            for (var i = 0; i < orderedRdfTriples.Count; i++)
            {
                var rdfTripleWithQueryIndex = orderedRdfTriples[i];
                var rdfTriple = rdfTripleWithQueryIndex.RDFTriple;
                var queryIndex = rdfTripleWithQueryIndex.QueryIndex;

                queryCursors = queryCursors.SetItem(
                    queryIndex,
                    new HasTypeCursorQueryCursor(
                        queryIndex,
                        PartitionPosition.Cursor,
                        new HasTypeCursorPartitionCursor(rdfTriple.Subject, rdfTriple.Partition)));

                var cursor = cursorSerializer.Serialize(Cursor.Create(ToHasTypeCursorItem(rdfTriple, queryCursors)));
                var edge = new RelayEdge<T>(cursor, nodeSelector(rdfTriple));
                allEdges = allEdges.Add(edge);

                // Update partition cursors
                var countInQuery = dataByQueryIndex[queryIndex]?.Items.Count ?? 0;
                var newIndexInQuery = dataPositionByQueryIndex[queryIndex] + 1;
                dataPositionByQueryIndex = dataPositionByQueryIndex.SetItem(queryIndex, newIndexInQuery);

                var queryHasNextPage = dataByQueryIndex[queryIndex]?.HasNextPage ?? false;
                if (newIndexInQuery >= countInQuery && !queryHasNextPage)
                {
                    queryCursors = queryCursors.SetItem(
                        queryIndex,
                        new HasTypeCursorQueryCursor(queryIndex, PartitionPosition.Finish, null));
                }
            }

            return allEdges;
        }

        public async Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValueAsync(
            GetConnectionByTypePropertyNameAndValueRequest request,
            CancellationToken cancellationToken)
        {
            var options = graphSettingsService.GetGraphSettings();
            var scanIndexForward = GetScanIndexForward(request.ConnectionArguments, request.OrderDesc);
            var limit = GetLimit(request.ConnectionArguments);
            var exclusiveStartKeyCursor = exclusiveStartKeyService.TryGetHasPropCursor(request.ConnectionArguments);
            var queryCount = options.PartitionCount;
            var rdfResponseArray = await Task.WhenAll(
                Enumerable
                    .Range(0, queryCount)
                    .Select(async queryIndex =>
                    {
                        if (!IsRequestRequired(queryIndex, exclusiveStartKeyCursor))
                        {
                            return null;
                        }

                        var partition = queryIndex.ToString(CultureInfo.InvariantCulture);
                        var rdfRequest = new QueryRDFTriplesByPartitionAndPredicateRequest(
                            options.TableName,
                            partition,
                            HasProp.PropertiesByTypePropertyNameAndValue(options.GraphName, request.TypeName, request.PropertyName, request.PropertyValue),
                            exclusiveStartKeyService.TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKey(request.ConnectionArguments, queryIndex, request.TypeName, request.PropertyName),
                            scanIndexForward,
                            limit,
                            true,
                            false);

                        return await rdfTripleStore.QueryRDFTriplesByPartitionAndPredicateAsync(rdfRequest, cancellationToken);
                    }));

            var rdfResponses = rdfResponseArray.ToImmutableList();
            var orderedRdfTriples = rdfResponses
                .SelectMany((response, queryIndex) => response?.Items.Select(rdfTriple => new RDFTripleWithQueryIndex(queryIndex, rdfTriple)) ?? [])
                .OrderByDirection(v => v.RDFTriple.Predicate, !scanIndexForward, StringComparer.Ordinal)
                .ThenBy(v => v.QueryIndex) // NOTE If the predicates match then ensure ordering by partition
                .ToImmutableList();

            var allEdges = ToHasPropRelayEdges(rdfResponses, orderedRdfTriples, exclusiveStartKeyCursor?.QueryCursors, v => v.Subject);

            var edges = allEdges
                .Take(limit)
                .ToImmutableList();

            var getNodesRequest = new GetNodesRequest(
                edges.Select(e => e.Node).ToImmutableList(),
                request.ConsistentRead);

            var getNodesResponse = await this.GetNodesAsync(getNodesRequest, cancellationToken);
            var nodesById = getNodesResponse.Nodes.ToImmutableDictionary(k => k.Node.Id);
            var nodeEdges = getNodesResponse
                .Nodes
                .Select((node, i) => new RelayEdge<INode>(edges[i].Cursor, node.Node))
                .ToImmutableList();

            var usedLast = request.ConnectionArguments.Last.HasValue;
            var pageInfo = new PageInfo(
                !usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r?.HasNextPage ?? false)),
                usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r?.HasNextPage ?? false)),
                edges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                edges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty);

            return new GetConnectionResponse(
                new Connection<RelayEdge<INode>, INode>(nodeEdges, pageInfo));
        }

        public async Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValuesAsync(
            GetConnectionByTypePropertyNameAndValuesRequest request,
            CancellationToken cancellationToken)
        {
            var options = graphSettingsService.GetGraphSettings();
            var scanIndexForward = GetScanIndexForward(request.ConnectionArguments, request.OrderDesc);
            var limit = GetLimit(request.ConnectionArguments);
            var exclusiveStartKeyCursor = exclusiveStartKeyService
                .TryGetHasPropCursor(request.ConnectionArguments);

            // The number of requests is the product of the partitions and values and is deterministic on subsequent pages
            // The cursors returned must track the state of each of these requests
            var queryCount = options.PartitionCount * request.PropertyValues.Count;
            var rdfResponseArray = await Task.WhenAll(
                Enumerable
                    .Range(0, queryCount)
                    .Select(async queryIndex =>
                    {
                        if (!IsRequestRequired(queryIndex, exclusiveStartKeyCursor))
                        {
                            return null;
                        }

                        var (partition, propertyValueIndex) = Math.DivRem(queryIndex, request.PropertyValues.Count);
                        var rdfRequest = new QueryRDFTriplesByPartitionAndPredicateRequest(
                            options.TableName,
                            partition.ToString(CultureInfo.InvariantCulture),
                            HasProp.PropertiesByTypePropertyNameAndValue(options.GraphName, request.TypeName, request.PropertyName, request.PropertyValues[propertyValueIndex]),
                            exclusiveStartKeyService.TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKey(request.ConnectionArguments, queryIndex, request.TypeName, request.PropertyName),
                            scanIndexForward,
                            limit,
                            true,
                            false);

                        return await rdfTripleStore.QueryRDFTriplesByPartitionAndPredicateAsync(rdfRequest, cancellationToken);
                    }));

            var rdfResponses = rdfResponseArray.ToImmutableList();
            var orderedRdfTriples = rdfResponses
                .SelectMany((response, queryIndex) => response?.Items.Select(rdfTriple => new RDFTripleWithQueryIndex(queryIndex, rdfTriple)) ?? [])
                .OrderByDirection(v => v.RDFTriple.Predicate, !scanIndexForward, StringComparer.Ordinal)
                .ThenBy(v => v.QueryIndex) // NOTE If the predicates match then ensure ordering by partition
                .ToImmutableList();

            var allEdges = ToHasPropRelayEdges(rdfResponses, orderedRdfTriples, exclusiveStartKeyCursor?.QueryCursors, v => v.Subject);

            var edges = allEdges
                .Take(limit)
                .ToImmutableList();

            var getNodesRequest = new GetNodesRequest(
                edges.Select(e => e.Node).ToImmutableList(),
                request.ConsistentRead);

            var getNodesResponse = await this.GetNodesAsync(getNodesRequest, cancellationToken);
            var nodesById = getNodesResponse.Nodes.ToImmutableDictionary(k => k.Node.Id);
            var nodeEdges = getNodesResponse
                .Nodes
                .Select((node, i) => new RelayEdge<INode>(edges[i].Cursor, node.Node))
                .ToImmutableList();

            var usedLast = request.ConnectionArguments.Last.HasValue;
            var pageInfo = new PageInfo(
                !usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r?.HasNextPage ?? false)),
                usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r?.HasNextPage ?? false)),
                edges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                edges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty);

            return new GetConnectionResponse(
                new Connection<RelayEdge<INode>, INode>(nodeEdges, pageInfo));
        }

        public async Task<ToEdgeQueryResponse> GetInToEdgeConnectionAsync(
            ToEdgeQueryRequest request,
            CancellationToken cancellationToken)
        {
            if (request.EdgeTypeName != null)
            {
                return await GetInToOneEdgeTypeConnectionAsync(request, cancellationToken);
            }

            return await GetInToAllEdgeTypesConnectionAsync(request, cancellationToken);
        }

        private static string GetInToOneEdgeTypeConnectionPredicate(
            GraphSettings options,
            ToEdgeQueryRequest request)
        {
            if (request.EdgeTypeName == null)
            {
                throw new GraphlessDBOperationException("Edge type name was expected");
            }

            if (request.FilterBy != null && request.OrderBy != null)
            {
                if (request.FilterBy.PropertyName != request.OrderBy.PropertyName)
                {
                    throw new NotSupportedException("Using different properties for filtering and ordering is not supported");
                }

                return HasInEdgeProp
                    .EdgesByTypeNodeInTypeEdgeTypeAndPropertyNameAndBeginsWithValue(options.GraphName, request.NodeTypeName, request.EdgeTypeName, request.FilterBy.PropertyName, request.FilterBy.PropertyOperator, request.FilterBy.PropertyValue)
                    .ToString();
            }

            if (request.FilterBy != null)
            {
                return HasInEdgeProp
                    .EdgesByTypeNodeInTypeEdgeTypeAndPropertyNameAndBeginsWithValue(options.GraphName, request.NodeTypeName, request.EdgeTypeName, request.FilterBy.PropertyName, request.FilterBy.PropertyOperator, request.FilterBy.PropertyValue)
                    .ToString();
            }

            if (request.OrderBy != null)
            {
                return HasInEdgeProp
                    .EdgesByTypeNodeInTypeEdgeTypeAndPropertyName(options.GraphName, request.NodeTypeName, request.EdgeTypeName, request.OrderBy.PropertyName)
                    .ToString();
            }

            return HasInEdge
                .EdgesByTypeNodeInTypeAndEdgeType(options.GraphName, request.NodeTypeName, request.EdgeTypeName)
                .ToString();
        }

        private async Task<ToEdgeQueryResponse> GetInToOneEdgeTypeConnectionAsync(
            ToEdgeQueryRequest request,
            CancellationToken cancellationToken)
        {
            if (request.EdgeTypeName == null)
            {
                throw new GraphlessDBOperationException("EdgeTypeName was expected");
            }

            var childPage = request.ConnectionArguments;
            var pagedConnection = request.NodeConnection;
            var options = graphSettingsService.GetGraphSettings();
            var scanIndexForward = GetScanIndexForward(request.ConnectionArguments, request.OrderBy?.Direction == OrderDirection.Desc);
            var limit = GetLimit(request.ConnectionArguments);
            var rdfResponses = await Task.WhenAll(pagedConnection
                .Edges
                .Select(async (nodeConnectionEdge, i) =>
                {
                    var nodeInKey = nodeConnectionEdge.Node.Id;

                    var exclusiveStartKey = i == 0
                        ? exclusiveStartKeyService.TryGetHasEdgeExclusiveStartKey(childPage, options, request)
                        : null;

                    var rdfRequest = new QueryRDFTriplesRequest(
                        options.TableName,
                        nodeInKey,
                        GetInToOneEdgeTypeConnectionPredicate(options, request),
                        exclusiveStartKey,
                        scanIndexForward,
                        limit,
                        request.ConsistentRead,
                        false);

                    var rdfResponse = await rdfTripleStore.QueryRDFTriplesAsync(
                        rdfRequest,
                        cancellationToken);

                    return new { Parent = nodeConnectionEdge, RdfResponse = rdfResponse };
                }));

            var rdfTripleAndParents = rdfResponses
                .SelectMany(rdfResponse => rdfResponse
                    .RdfResponse
                    .Items
                    .Select(rdfTriple =>
                    {
                        return new { rdfResponse.Parent, RdfTriple = rdfTriple };
                    })
                )
                .ToImmutableList();

            var allEdges = rdfTripleAndParents
                .Select(r =>
                {
                    if (HasInEdge.IsPredicate(r.RdfTriple.Predicate))
                    {
                        var predicate = HasInEdge.Parse(r.RdfTriple.Predicate);
                        if (r.RdfTriple.Subject != predicate.NodeInId)
                        {
                            throw new GraphlessDBOperationException("Invalid predicate for subject");
                        }

                        var cursor = cursorSerializer.Deserialize(r.Parent.Cursor);
                        cursor = cursor.AddAsParentToRoot(ToCursorItem(r.RdfTriple, []));
                        var node = new EdgeKey(predicate.EdgeTypeName, predicate.NodeInId, predicate.NodeOutId);
                        return new RelayEdge<EdgeKey>(cursorSerializer.Serialize(cursor), node);
                    }

                    if (HasInEdgeProp.IsPredicate(r.RdfTriple.Predicate))
                    {
                        var predicate = HasInEdgeProp.Parse(r.RdfTriple.Predicate);
                        if (r.RdfTriple.Subject != predicate.NodeInId)
                        {
                            throw new GraphlessDBOperationException("Invalid predicate for subject");
                        }

                        var cursor = cursorSerializer.Deserialize(r.Parent.Cursor);
                        cursor = cursor.AddAsParentToRoot(ToCursorItem(r.RdfTriple, []));
                        var node = new EdgeKey(predicate.EdgeTypeName, predicate.NodeInId, predicate.NodeOutId);
                        return new RelayEdge<EdgeKey>(cursorSerializer.Serialize(cursor), node);
                    }

                    throw new GraphlessDBOperationException("Expected HasInEdge or HasInEdgeProp type");
                })
                .ToImmutableList();

            var edges = allEdges
                .Take(limit)
                .ToImmutableList();

            var usedLast = request.ConnectionArguments?.Last.HasValue ?? false;
            var pageInfo = new PageInfo(
                !usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r.RdfResponse.HasNextPage)),
                usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r.RdfResponse.HasNextPage)),
                edges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                edges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty);

            return new ToEdgeQueryResponse(
                new Connection<RelayEdge<EdgeKey>, EdgeKey>(edges, pageInfo));
        }


        private async Task<ToEdgeQueryResponse> GetInToAllEdgeTypesConnectionAsync(
            ToEdgeQueryRequest request,
            CancellationToken cancellationToken)
        {
            var options = graphSettingsService.GetGraphSettings();
            var exclusiveStartKey = exclusiveStartKeyService.TryGetHasEdgeExclusiveStartKey(request.ConnectionArguments, options, request);
            var scanIndexForward = GetScanIndexForward(request.ConnectionArguments, request.OrderBy?.Direction == OrderDirection.Desc);
            var limit = GetLimit(request.ConnectionArguments);
            var rdfResponses = await Task.WhenAll(request
                .NodeConnection
                .Edges
                .Select(async nodeConnectionEdge =>
                {
                    if (request.OrderBy != null || request.FilterBy != null)
                    {
                        throw new NotSupportedException("Ordering and / or filtering when searching for all edge types is not supported");
                    }

                    var rdfRequest = new QueryRDFTriplesRequest(
                        options.TableName,
                        nodeConnectionEdge.Node.Id,
                        HasInEdge.EdgesByTypeNodeInType(options.GraphName, request.NodeTypeName),
                        exclusiveStartKey,
                        scanIndexForward,
                        limit,
                        request.ConsistentRead,
                        false);

                    return await rdfTripleStore.QueryRDFTriplesAsync(rdfRequest, cancellationToken);
                }));

            var allEdges = rdfResponses
                .SelectMany(response => response.Items)
                .OrderByDirection(item => item.Predicate, !scanIndexForward, StringComparer.Ordinal)
                .Select(rdfTriple =>
                {
                    var predicate = HasInEdge.Parse(rdfTriple.Predicate);
                    if (rdfTriple.Subject != predicate.NodeInId)
                    {
                        throw new GraphlessDBOperationException("Invalid predicate for subject");
                    }

                    var cursorItem = ToCursorItem(rdfTriple, []);
                    return new RelayEdge<EdgeKey>(
                        cursorSerializer.Serialize(Cursor.Create(cursorItem)),
                        new EdgeKey(predicate.EdgeTypeName, predicate.NodeInId, predicate.NodeOutId));
                })
                .ToImmutableList();

            var edges = allEdges
                .Take(limit)
                .ToImmutableList();

            var usedLast = request.ConnectionArguments?.Last.HasValue ?? false;

            var pageInfo = new PageInfo(
                !usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r.HasNextPage)),
                usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r.HasNextPage)),
                edges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                edges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty);

            return new ToEdgeQueryResponse(
                new Connection<RelayEdge<EdgeKey>, EdgeKey>(edges, pageInfo));
        }

        public async Task<ToEdgeQueryResponse> GetOutToEdgeConnectionAsync(
            ToEdgeQueryRequest request,
            CancellationToken cancellationToken)
        {
            if (request.EdgeTypeName != null)
            {
                return await GetOutToOneEdgeTypeConnectionAsync(request, cancellationToken);
            }

            return await GetOutToAllEdgeTypesConnectionAsync(request, cancellationToken);
        }

        public async Task<ToEdgeQueryResponse> GetInAndOutToEdgeConnectionAsync(
            ToEdgeQueryRequest request,
            CancellationToken cancellationToken)
        {
            if (request.EdgeTypeName != null)
            {
                return await GetInAndOutToOneEdgeTypeConnectionAsync(request, cancellationToken);
            }

            throw new NotSupportedException("In and out to all edge types is not supported");
            // return await GetInAndOutToAllEdgeTypesConnectionAsync(request, cancellationToken);
        }

        private async Task<ToEdgeQueryResponse> GetOutToAllEdgeTypesConnectionAsync(
            ToEdgeQueryRequest request,
            CancellationToken cancellationToken)
        {
            var options = graphSettingsService.GetGraphSettings();
            var exclusiveStartKey = exclusiveStartKeyService.TryGetHasEdgeExclusiveStartKey(request.ConnectionArguments, options, request);
            var scanIndexForward = GetScanIndexForward(request.ConnectionArguments, request.OrderBy?.Direction == OrderDirection.Desc);
            var limit = GetLimit(request.ConnectionArguments);
            var rdfResponses = await Task.WhenAll(request
                .NodeConnection
                .Edges
                .Select(async nodeConnectionEdge =>
                {
                    var rdfRequest = new QueryRDFTriplesRequest(
                        options.TableName,
                        nodeConnectionEdge.Node.Id,
                        HasOutEdge.EdgesByTypeNodeOutType(options.GraphName, request.NodeTypeName),
                        exclusiveStartKey,
                        scanIndexForward,
                        limit,
                        request.ConsistentRead,
                        false);

                    return await rdfTripleStore.QueryRDFTriplesAsync(rdfRequest, cancellationToken);
                }));

            var rdfTriples = rdfResponses
                .SelectMany(response => response.Items)
                .OrderByDirection(item => item.Predicate, !scanIndexForward, StringComparer.Ordinal)
                .ToImmutableList();

            var allEdges = rdfTriples
                .Select(rdfTriple =>
                {
                    var predicate = HasOutEdge.Parse(rdfTriple.Predicate);
                    if (rdfTriple.Subject != predicate.NodeOutId)
                    {
                        throw new GraphlessDBOperationException("Invalid predicate for subject");
                    }

                    var cursorItem = ToCursorItem(rdfTriple, []);
                    return new RelayEdge<EdgeKey>(
                        cursorSerializer.Serialize(Cursor.Create(cursorItem)),
                        new EdgeKey(predicate.EdgeTypeName, predicate.NodeInId, predicate.NodeOutId));
                })
                .ToImmutableList();

            var edges = allEdges
                .Take(limit)
                .ToImmutableList();

            var usedLast = request.ConnectionArguments?.Last.HasValue ?? false;

            var pageInfo = new PageInfo(
                !usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r.HasNextPage)),
                usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r.HasNextPage)),
                edges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                edges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty);

            return new ToEdgeQueryResponse(
                new Connection<RelayEdge<EdgeKey>, EdgeKey>(edges, pageInfo));
        }

        private static string GetOutToOneEdgeTypeConnectionPredicate(GraphSettings options, ToEdgeQueryRequest request)
        {
            if (request.EdgeTypeName == null)
            {
                throw new GraphlessDBOperationException("Edge type name was expected");
            }

            if (request.FilterBy != null && request.OrderBy != null)
            {
                if (request.FilterBy.PropertyName != request.OrderBy.PropertyName)
                {
                    throw new NotSupportedException("Using different properties for filtering and ordering is not supported");
                }

                return HasOutEdgeProp
                    .EdgesByTypeNodeOutTypeEdgeTypeAndPropertyNameAndBeginsWithValue(options.GraphName, request.NodeTypeName, request.EdgeTypeName, request.FilterBy.PropertyName, request.FilterBy.PropertyOperator, request.FilterBy.PropertyValue)
                    .ToString();
            }

            if (request.FilterBy != null)
            {
                return HasOutEdgeProp
                    .EdgesByTypeNodeOutTypeEdgeTypeAndPropertyNameAndBeginsWithValue(options.GraphName, request.NodeTypeName, request.EdgeTypeName, request.FilterBy.PropertyName, request.FilterBy.PropertyOperator, request.FilterBy.PropertyValue)
                    .ToString();
            }

            if (request.OrderBy != null)
            {
                return HasOutEdgeProp
                    .EdgesByTypeNodeOutTypeEdgeTypeAndPropertyName(options.GraphName, request.NodeTypeName, request.EdgeTypeName, request.OrderBy.PropertyName)
                    .ToString();
            }

            return HasOutEdge
                .EdgesByTypeNodeOutTypeAndEdgeType(options.GraphName, request.NodeTypeName, request.EdgeTypeName)
                .ToString();
        }

        public async Task<ToEdgeQueryResponse> GetOutToOneEdgeTypeConnectionAsync(
            ToEdgeQueryRequest request,
            CancellationToken cancellationToken)
        {
            if (request.EdgeTypeName == null)
            {
                throw new GraphlessDBOperationException("EdgeTypeName was expected");
            }

            var childPage = request.ConnectionArguments;
            var pagedConnection = request.NodeConnection;
            var options = graphSettingsService.GetGraphSettings();
            var scanIndexForward = GetScanIndexForward(request.ConnectionArguments, request.OrderBy?.Direction == OrderDirection.Desc);
            var limit = GetLimit(request.ConnectionArguments);
            var rdfResponses = await Task.WhenAll(pagedConnection
                .Edges
                .Select(async (nodeConnectionEdge, i) =>
                {
                    var nodeOutKey = nodeConnectionEdge.Node.Id;

                    var exclusiveStartKey = i == 0
                        ? exclusiveStartKeyService.TryGetHasEdgeExclusiveStartKey(childPage, options, request)
                        : null;

                    var rdfRequest = new QueryRDFTriplesRequest(
                        options.TableName,
                        nodeOutKey,
                        GetOutToOneEdgeTypeConnectionPredicate(options, request),
                        exclusiveStartKey,
                        scanIndexForward,
                        limit,
                        request.ConsistentRead,
                        false);

                    var rdfResponse = await rdfTripleStore.QueryRDFTriplesAsync(
                        rdfRequest,
                        cancellationToken);

                    return new { Parent = nodeConnectionEdge, RdfResponse = rdfResponse };
                }));

            var rdfTripleAndParents = rdfResponses
                .SelectMany(rdfResponse => rdfResponse
                    .RdfResponse
                    .Items
                    .Select(rdfTriple =>
                    {
                        return new { rdfResponse.Parent, RdfTriple = rdfTriple };
                    }))
                .ToImmutableList();

            var allEdges = rdfTripleAndParents
                .Select(r =>
                {
                    if (HasOutEdge.IsPredicate(r.RdfTriple.Predicate))
                    {
                        var predicate = HasOutEdge.Parse(r.RdfTriple.Predicate);
                        if (r.RdfTriple.Subject != predicate.NodeOutId)
                        {
                            throw new GraphlessDBOperationException("Invalid predicate for subject");
                        }

                        var cursor = cursorSerializer.Deserialize(r.Parent.Cursor);
                        cursor = cursor.AddAsParentToRoot(ToCursorItem(r.RdfTriple, []));
                        var node = new EdgeKey(predicate.EdgeTypeName, predicate.NodeInId, predicate.NodeOutId);
                        return new RelayEdge<EdgeKey>(cursorSerializer.Serialize(cursor), node);
                    }

                    if (HasOutEdgeProp.IsPredicate(r.RdfTriple.Predicate))
                    {
                        var predicate = HasOutEdgeProp.Parse(r.RdfTriple.Predicate);
                        if (r.RdfTriple.Subject != predicate.NodeOutId)
                        {
                            throw new GraphlessDBOperationException("Invalid predicate for subject");
                        }

                        var cursor = cursorSerializer.Deserialize(r.Parent.Cursor);
                        cursor = cursor.AddAsParentToRoot(ToCursorItem(r.RdfTriple, []));
                        var node = new EdgeKey(predicate.EdgeTypeName, predicate.NodeInId, predicate.NodeOutId);
                        return new RelayEdge<EdgeKey>(cursorSerializer.Serialize(cursor), node);
                    }

                    throw new GraphlessDBOperationException("Expected HasOutEdge or HasOutEdgeProp type");
                })
                .ToImmutableList();

            var edges = allEdges
                .Take(limit)
                .ToImmutableList();

            var usedLast = request.ConnectionArguments?.Last.HasValue ?? false;
            var pageInfo = new PageInfo(
               !usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r.RdfResponse.HasNextPage)),
                usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r.RdfResponse.HasNextPage)),
                edges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                edges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty);

            return new ToEdgeQueryResponse(
                new Connection<RelayEdge<EdgeKey>, EdgeKey>(edges, pageInfo));
        }

        public async Task<ToEdgeQueryResponse> GetInAndOutToOneEdgeTypeConnectionAsync(
            ToEdgeQueryRequest request,
            CancellationToken cancellationToken)
        {
            if (request.EdgeTypeName == null)
            {
                throw new GraphlessDBOperationException("EdgeTypeName was expected");
            }

            var childPage = request.ConnectionArguments;
            var pagedConnection = request.NodeConnection;
            var options = graphSettingsService.GetGraphSettings();
            var scanIndexForward = GetScanIndexForward(request.ConnectionArguments, request.OrderBy?.Direction == OrderDirection.Desc);
            var limit = GetLimit(request.ConnectionArguments);

            // TODO: THIS FUNCTIONALITY IS WRONG
            // NOTE: Always deterministically process the IN before the OUT
            // so that the IN / OUT specific exclusive start key doesnt get used incorrectly
            var rdfResponses = await Task.WhenAll(pagedConnection
                .Edges
                .Select(async (nodeConnectionEdge, i) =>
                {
                    var nodeKey = nodeConnectionEdge.Node.Id;

                    var exclusiveStartKey = i == 0
                        ? exclusiveStartKeyService.TryGetHasEdgeExclusiveStartKey(childPage, options, request)
                        : null;

                    QueryRDFTriplesResponse? responseIn = null;
                    var predicateIn = GetInToOneEdgeTypeConnectionPredicate(options, request);
                    if (exclusiveStartKey == null || exclusiveStartKey.Predicate.StartsWith(predicateIn, StringComparison.Ordinal))
                    {
                        var rdfRequestIn = new QueryRDFTriplesRequest(
                            options.TableName,
                            nodeKey,
                            predicateIn,
                            exclusiveStartKey,
                            scanIndexForward,
                            limit,
                            request.ConsistentRead,
                            false);

                        responseIn = await rdfTripleStore.QueryRDFTriplesAsync(
                            rdfRequestIn,
                            cancellationToken);
                    }

                    QueryRDFTriplesResponse? responseOut = null;
                    var predicateOut = GetOutToOneEdgeTypeConnectionPredicate(options, request);
                    if (exclusiveStartKey != null && exclusiveStartKey.Predicate.StartsWith(predicateOut, StringComparison.Ordinal) || false)
                    {
                        var rdfRequestOut = new QueryRDFTriplesRequest(
                            options.TableName,
                            nodeKey,
                            predicateOut,
                            exclusiveStartKey,
                            scanIndexForward,
                            limit,
                            request.ConsistentRead,
                            false);

                        responseOut = await rdfTripleStore.QueryRDFTriplesAsync(
                            rdfRequestOut,
                            cancellationToken);
                    }

                    return new { Parent = nodeConnectionEdge, RdfRequestIn = responseIn, RdfResponseOut = responseOut };
                }));

            var rdfTripleAndParents = rdfResponses
                .SelectMany(rdfResponse =>
                {
                    if (rdfResponse.RdfRequestIn != null && rdfResponse.RdfResponseOut != null)
                    {
                        return rdfResponse.RdfRequestIn.Items.Select(rdfTriple =>
                        {
                            return new { rdfResponse.Parent, RdfTriple = rdfTriple };
                        }).Concat(rdfResponse.RdfResponseOut.Items.Select(rdfTriple =>
                        {
                            return new { rdfResponse.Parent, RdfTriple = rdfTriple };
                        }));
                    }

                    if (rdfResponse.RdfRequestIn != null)
                    {
                        return rdfResponse.RdfRequestIn.Items.Select(rdfTriple =>
                        {
                            return new { rdfResponse.Parent, RdfTriple = rdfTriple };
                        });
                    }

                    if (rdfResponse.RdfResponseOut != null)
                    {
                        return rdfResponse.RdfResponseOut.Items.Select(rdfTriple =>
                        {
                            return new { rdfResponse.Parent, RdfTriple = rdfTriple };
                        });
                    }

                    throw new NotSupportedException();
                })
                .ToImmutableList();

            var allEdges = rdfTripleAndParents
                .Select(r =>
                {
                    if (HasInEdge.IsPredicate(r.RdfTriple.Predicate))
                    {
                        var predicate = HasInEdge.Parse(r.RdfTriple.Predicate);
                        if (r.RdfTriple.Subject != predicate.NodeInId)
                        {
                            throw new GraphlessDBOperationException("Invalid predicate for subject");
                        }

                        var cursor = cursorSerializer.Deserialize(r.Parent.Cursor);
                        cursor = cursor.AddAsParentToRoot(ToCursorItem(r.RdfTriple, []));
                        var node = new EdgeKey(predicate.EdgeTypeName, predicate.NodeInId, predicate.NodeOutId);
                        return new RelayEdge<EdgeKey>(cursorSerializer.Serialize(cursor), node);
                    }

                    if (HasOutEdge.IsPredicate(r.RdfTriple.Predicate))
                    {
                        var predicate = HasOutEdge.Parse(r.RdfTriple.Predicate);
                        if (r.RdfTriple.Subject != predicate.NodeOutId)
                        {
                            throw new GraphlessDBOperationException("Invalid predicate for subject");
                        }

                        var cursor = cursorSerializer.Deserialize(r.Parent.Cursor);
                        cursor = cursor.AddAsParentToRoot(ToCursorItem(r.RdfTriple, []));
                        var node = new EdgeKey(predicate.EdgeTypeName, predicate.NodeInId, predicate.NodeOutId);
                        return new RelayEdge<EdgeKey>(cursorSerializer.Serialize(cursor), node);
                    }

                    if (HasInEdgeProp.IsPredicate(r.RdfTriple.Predicate))
                    {
                        var predicate = HasInEdgeProp.Parse(r.RdfTriple.Predicate);
                        if (r.RdfTriple.Subject != predicate.NodeInId)
                        {
                            throw new GraphlessDBOperationException("Invalid predicate for subject");
                        }

                        var cursor = cursorSerializer.Deserialize(r.Parent.Cursor);
                        cursor = cursor.AddAsParentToRoot(ToCursorItem(r.RdfTriple, []));
                        var node = new EdgeKey(predicate.EdgeTypeName, predicate.NodeInId, predicate.NodeOutId);
                        return new RelayEdge<EdgeKey>(cursorSerializer.Serialize(cursor), node);
                    }

                    if (HasOutEdgeProp.IsPredicate(r.RdfTriple.Predicate))
                    {
                        var predicate = HasOutEdgeProp.Parse(r.RdfTriple.Predicate);
                        if (r.RdfTriple.Subject != predicate.NodeOutId)
                        {
                            throw new GraphlessDBOperationException("Invalid predicate for subject");
                        }

                        var cursor = cursorSerializer.Deserialize(r.Parent.Cursor);
                        cursor = cursor.AddAsParentToRoot(ToCursorItem(r.RdfTriple, []));
                        var node = new EdgeKey(predicate.EdgeTypeName, predicate.NodeInId, predicate.NodeOutId);
                        return new RelayEdge<EdgeKey>(cursorSerializer.Serialize(cursor), node);
                    }

                    throw new GraphlessDBOperationException("Expected HasInEdge, HasInEdgeProp, HasOutEdge or HasOutEdgeProp type");
                })
                .DistinctBy(e => e.Node) // Bug fix incase in and out result in the same edge
                .ToImmutableList();

            var edges = allEdges
                .Take(limit)
                .ToImmutableList();

            var usedLast = request.ConnectionArguments?.Last.HasValue ?? false;
            var pageInfo = new PageInfo(
                !usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r.RdfRequestIn != null && r.RdfRequestIn.HasNextPage || r.RdfResponseOut != null && r.RdfResponseOut.HasNextPage)),
                usedLast && (allEdges.Count > edges.Count || rdfResponses.Any(r => r.RdfRequestIn != null && r.RdfRequestIn.HasNextPage || r.RdfResponseOut != null && r.RdfResponseOut.HasNextPage)),
                edges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                edges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty);

            return new ToEdgeQueryResponse(
                new Connection<RelayEdge<EdgeKey>, EdgeKey>(edges, pageInfo));
        }

        public async Task PutAsync(
            PutRequest request,
            CancellationToken cancellationToken)
        {
            if (!request.EdgeByPropChecks.IsEmpty)
            {
                throw new NotSupportedException("EdgeByPropChecks are not currently supported");
            }

            var nodeEdgeUpdatesByKey = request
                .PutEntities
                .OfType<IEdge>()
                .SelectMany(e =>
                {
                    return new[] {
                        e.InId,
                        e.OutId,
                    };
                })
                .Distinct()
                .ToImmutableHashSet();

            var checkNodesByKey = request
                .AllEdgesCheckForNodes
                .ToImmutableDictionary(k => k.Id);

            var putNodesByKey = request
                .PutEntities
                .OfType<INode>()
                .Select(n => n.ToKey())
                .ToImmutableHashSet();

            var nodesWithoutEdgeChecks = nodeEdgeUpdatesByKey
                .Where(v => !putNodesByKey.Contains(v) && !checkNodesByKey.ContainsKey(v))
                .ToImmutableList();

            var noEdgeChecksForNodeIdSet = request.NoEdgeChecksForNodeIds.ToImmutableHashSet();
            if (!request.WithoutNodeEdgeChecks && nodesWithoutEdgeChecks.Where(nodeId => !noEdgeChecksForNodeIdSet.Contains(nodeId)).Any())
            {
                throw new GraphlessDBOperationException("Specify WithoutNodeEdgeChecks or all nodes which have edge updates must be supplied in either PutEntities or CheckNodes");
            }

            var putEntityGroups = await Task.WhenAll(request
                .PutEntities
                .Select(async entity =>
                {
                    var checkNodeAndEdges = checkNodesByKey.ContainsKey(entity.ToKey());
                    return await GetWriteRDFTriplesAsync(entity, checkNodeAndEdges, cancellationToken);
                }));

            var putEntities = putEntityGroups
                .SelectMany(e => e)
                .ToImmutableList();

            var nodesWithoutEdgeChecksSet = nodesWithoutEdgeChecks.ToHashSet();

            var nodeEdgeUpdateEntities = nodeEdgeUpdatesByKey
                .Where(nodeEdgeUpdateKey => !putNodesByKey.Contains(nodeEdgeUpdateKey) && !nodesWithoutEdgeChecksSet.Contains(nodeEdgeUpdateKey))
                .Select(nodeEdgeUpdateKey => GetUpdateTransactWriteItemForAllEdgeIncrementWithCheck(checkNodesByKey[nodeEdgeUpdateKey], true, true))
                .ToImmutableList();

            var autoNodeEdgeUpdates = nodesWithoutEdgeChecks
                .Select(GetUpdateTransactWriteItemForAllEdgeIncrementWithoutCheck)
                .ToImmutableList();

            var checkEntities = request
                .AllEdgesCheckForNodes
                .Where(node => !putNodesByKey.Contains(node.ToKey()) && !nodeEdgeUpdatesByKey.Contains(node.ToKey()))
                .Select(node => GetCheckTransactionWriteItem(node, true, true))
                .ToImmutableList();

            var writeEntities = putEntities
                .AddRange(nodeEdgeUpdateEntities)
                .AddRange(checkEntities)
                .AddRange(autoNodeEdgeUpdates);

            var dataRequest = new WriteRDFTriplesRequest(request.MutationId.Next(), false, writeEntities);

            await rdfTripleStore.WriteRDFTriplesAsync(dataRequest, cancellationToken);
        }

        public async Task<ImmutableList<RDFTriple>> GetPropRdfTriplesAsync(
            string nodeId,
            CancellationToken cancellationToken)
        {
            var options = graphSettingsService.GetGraphSettings();

            var response = await rdfTripleStore.QueryRDFTriplesAsync(
                new QueryRDFTriplesRequest(
                    options.TableName,
                    nodeId,
                    HasProp.PropertiesByType(options.GraphName, GlobalId.Parse(nodeId).TypeName),
                    null,
                    true,
                    1000,
                    true,
                    false),
                cancellationToken);

            return response.Items;
        }

        private async Task<ImmutableList<RDFTriple>> GetHasEdgePropRdfTriplesAsync(
            string edgeTypeName,
            string nodeInId,
            string nodeOutId,
            CancellationToken cancellationToken)
        {
            var hasInTask = GetHasInEdgePropRdfTriplesAsync(edgeTypeName, nodeInId, nodeOutId, cancellationToken);
            var hasOutTask = GetHasOutEdgePropRdfTriplesAsync(edgeTypeName, nodeInId, nodeOutId, cancellationToken);
            var hasIn = await hasInTask;
            var hasOut = await hasOutTask;
            return hasIn.AddRange(hasOut);
        }

        private async Task<ImmutableList<RDFTriple>> GetHasInEdgePropRdfTriplesAsync(
            string edgeTypeName,
            string nodeInId,
            string nodeOutId,
            CancellationToken cancellationToken)
        {
            var options = graphSettingsService.GetGraphSettings();

            var response = await rdfTripleStore.QueryRDFTriplesAsync(
                new QueryRDFTriplesRequest(
                    options.TableName,
                    nodeInId,
                    HasInEdgeProp.EdgesByTypeNodeInTypeAndEdgeType(options.GraphName, GlobalId.Parse(nodeInId).TypeName, edgeTypeName),
                    null,
                    true,
                    1000,
                    true,
                    false),
                cancellationToken);

            // TODO Currently post filtering
            return response
                .Items
                .Where(t =>
                {
                    var predicate = HasInEdgeProp.Parse(t.Predicate);
                    return predicate.NodeInId == nodeInId && predicate.NodeOutId == nodeOutId;
                })
                .ToImmutableList();
        }

        private async Task<ImmutableList<RDFTriple>> GetHasOutEdgePropRdfTriplesAsync(
            string edgeTypeName,
            string nodeInId,
            string nodeOutId,
            CancellationToken cancellationToken)
        {
            var options = graphSettingsService.GetGraphSettings();

            var response = await rdfTripleStore.QueryRDFTriplesAsync(
                new QueryRDFTriplesRequest(
                    options.TableName,
                    nodeOutId,
                    HasOutEdgeProp.EdgesByTypeNodeOutTypeAndEdgeType(options.GraphName, GlobalId.Parse(nodeOutId).TypeName, edgeTypeName),
                    null,
                    true,
                    1000,
                    true,
                    false),
                cancellationToken);

            // TODO Currently post filtering
            return response
                .Items
                .Where(t =>
                {
                    var predicate = HasOutEdgeProp.Parse(t.Predicate);
                    return predicate.NodeInId == nodeInId && predicate.NodeOutId == nodeOutId;
                })
                .ToImmutableList();
        }

        private Task<ImmutableList<WriteRDFTriple>> GetWriteRDFTriplesAsync(IEntity entity, bool checkNodeAndEdges, CancellationToken cancellationToken)
        {
            return entity switch
            {
                INode node when GetOperation(node) == PutOperation.Create => Task.FromResult(GetCreateWriteRDFTriple(node, false, checkNodeAndEdges)),
                INode node when GetOperation(node) == PutOperation.Update => GetUpdateTransactWriteItemsAsync(node, true, checkNodeAndEdges, cancellationToken),
                INode node when GetOperation(node) == PutOperation.Delete => Task.FromResult(GetDeleteTransactWriteItems(node, true, checkNodeAndEdges)),
                IEdge edge when GetOperation(edge) == PutOperation.Create => Task.FromResult(GetCreateTransactWriteItems(edge, false, checkNodeAndEdges)),
                IEdge edge when GetOperation(edge) == PutOperation.Update => GetUpdateTransactWriteItemsAsync(edge, cancellationToken),
                IEdge edge when GetOperation(edge) == PutOperation.Delete => Task.FromResult(GetDeleteTransactWriteItems(edge, true, checkNodeAndEdges)),
                _ => throw new NotSupportedException()
            };
        }

        private WriteRDFTriple GetCheckTransactionWriteItem(INode node, bool checkNode, bool checkAllEdges)
        {
            return WriteRDFTriple.Create(
                new CheckRDFTripleVersion(
                    graphSettingsService.GetGraphSettings().TableName,
                    rdfTripleFactory.GetHasTypeRDFTriple(node).AsKey(),
                    new VersionDetailCondition(
                        checkNode ? node.Version.NodeVersion : null,
                        checkAllEdges ? node.Version.AllEdgesVersion : null)));
        }

        private static PutOperation GetOperation(INode node)
        {
            if (node.Version.NodeVersion == 0)
            {
                return PutOperation.Create;
            }

            if (node.DeletedAt != DateTime.MinValue)
            {
                return PutOperation.Delete;
            }

            return PutOperation.Update;
        }

        private static PutOperation GetOperation(IEdge edge)
        {
            if (edge.DeletedAt != DateTime.MinValue)
            {
                return PutOperation.Delete;
            }

            if (edge.CreatedAt == edge.UpdatedAt)
            {
                return PutOperation.Create;
            }

            return PutOperation.Update;
        }

        private ImmutableList<WriteRDFTriple> GetCreateWriteRDFTriple(INode node, bool checkNode, bool checkAllEdges)
        {
            if (checkNode || checkAllEdges)
            {
                throw new NotSupportedException("Checking node and edges on a new entity is not supported");
            }

            var options = graphSettingsService.GetGraphSettings();

            var rdfTriples = rdfTripleFactory
                .GetHasPropRDFTriples(node)
                .Add(rdfTripleFactory.GetHasTypeRDFTriple(node))
                .Add(rdfTripleFactory.GetHasBlobRDFTriple(node));

            return rdfTriples
                .Select(rdfTriple => WriteRDFTriple.Create(new AddRDFTriple(options.TableName, rdfTriple)))
                .ToImmutableList();
        }

        private ImmutableList<WriteRDFTriple> GetCreateTransactWriteItems(IEdge edge, bool checkNode, bool checkAllEdges)
        {
            if (checkNode || checkAllEdges)
            {
                throw new NotSupportedException("Checking node and edges on a new entity is not supported");
            }

            var options = graphSettingsService.GetGraphSettings();

            var rdfTriples = rdfTripleFactory
                .GetHasEdgeRDFTriples(edge)
                .AddRange(rdfTripleFactory.GetHasEdgePropRDFTriples(edge));

            return rdfTriples
                .Select(rdfTriple => WriteRDFTriple.Create(
                    new AddRDFTriple(
                        options.TableName,
                        rdfTriple)))
                .ToImmutableList();
        }

        private WriteRDFTriple GetUpdateTransactWriteItemForAllEdgeIncrementWithoutCheck(string id)
        {
            var options = graphSettingsService.GetGraphSettings();

            return WriteRDFTriple.Create(
                new IncrementRDFTripleAllEdgesVersion(
                    options.TableName,
                    new RDFTripleKey(id, new HasType(options.GraphName, GlobalId.Parse(id).TypeName, id).ToString()),
                    VersionDetailCondition.None));
        }

        private WriteRDFTriple GetUpdateTransactWriteItemForAllEdgeIncrementWithCheck(
            INode node, bool checkNode, bool checkAllEdges)
        {
            return WriteRDFTriple.Create(
                new UpdateRDFTripleAllEdgesVersion(
                    graphSettingsService.GetGraphSettings().TableName,
                    rdfTripleFactory.GetHasTypeRDFTriple(node).AsKey(),
                    new VersionDetailCondition(
                        checkNode ? node.Version.NodeVersion : null,
                        checkAllEdges ? node.Version.AllEdgesVersion : null),
                    node.Version.AllEdgesVersion + 1));
        }

        private async Task<ImmutableList<WriteRDFTriple>> GetUpdateTransactWriteItemsAsync(
            INode node, bool checkNode, bool checkAllEdges, CancellationToken cancellationToken)
        {
            var options = graphSettingsService.GetGraphSettings();
            var newPropRdfTriples = rdfTripleFactory.GetHasPropRDFTriples(node);
            var existingPropRdfTriples = await GetPropRdfTriplesAsync(node.Id, cancellationToken);
            var newPropRdfTriplesByPredicate = newPropRdfTriples.ToImmutableDictionary(k => k.Predicate);
            var existingPropRdfTriplesByPredicate = existingPropRdfTriples.ToImmutableDictionary(k => k.Predicate);

            // Update the HasType RDFTriple
            var hasTypeRDFTriple = WriteRDFTriple.Create(
                new UpdateRDFTriple(
                    options.TableName,
                    rdfTripleFactory.GetHasTypeRDFTriple(node),
                    new VersionDetailCondition(
                        checkNode ? node.Version.NodeVersion - 1 : null,
                        checkAllEdges ? node.Version.AllEdgesVersion : null)));

            // Add a new HasBlob RDFTriple
            var hasBlobRDFTriple = WriteRDFTriple.Create(
                new AddRDFTriple(
                    options.TableName,
                    rdfTripleFactory.GetHasBlobRDFTriple(node)));

            // Remove property RDFTriples which arent in target state but are in current state
            var deletedPropRDFTriples = existingPropRdfTriples
                .Where(existingPropRdfTriple => !newPropRdfTriplesByPredicate.ContainsKey(existingPropRdfTriple.Predicate))
                .Select(existingPropRdfTriple => WriteRDFTriple.Create(new DeleteRDFTriple(
                    options.TableName,
                    existingPropRdfTriple.AsKey(),
                    VersionDetailCondition.None)))
                .ToImmutableList();

            // Add property RDFTriple which are in target state and not in current state
            var addedPropRDFTriples = newPropRdfTriples
                .Where(newPropRdfTriple => !existingPropRdfTriplesByPredicate.ContainsKey(newPropRdfTriple.Predicate))
                .Select(newPropRdfTriple => WriteRDFTriple.Create(new AddRDFTriple(
                    options.TableName,
                    newPropRdfTriple)))
                .ToImmutableList();

            return [hasTypeRDFTriple, hasBlobRDFTriple, .. deletedPropRDFTriples, .. addedPropRDFTriples];
        }

        private async Task<ImmutableList<WriteRDFTriple>> GetUpdateTransactWriteItemsAsync(
            IEdge edge, CancellationToken cancellationToken)
        {
            var options = graphSettingsService.GetGraphSettings();
            var newPropRdfTriples = rdfTripleFactory.GetHasEdgePropRDFTriples(edge);
            var existingPropRdfTriples = await GetHasEdgePropRdfTriplesAsync(edge.GetType().Name, edge.InId, edge.OutId, cancellationToken);
            var newPropRdfTriplesByPredicate = newPropRdfTriples.ToImmutableDictionary(k => k.Predicate);
            var existingPropRdfTriplesByPredicate = existingPropRdfTriples.ToImmutableDictionary(k => k.Predicate);

            var comparison = existingPropRdfTriplesByPredicate
                .CompareTo(newPropRdfTriplesByPredicate, v => v, v => v);

            var hasEdgeRDFTriples = rdfTripleFactory
                .GetHasEdgeRDFTriples(edge)
                .Select(rdfTriple => WriteRDFTriple.Create(
                    new UpdateRDFTriple(
                        options.TableName,
                        rdfTriple,
                        VersionDetailCondition.None)))
                .ToImmutableList();

            // Remove property RDFTriples which arent in target state but are in current state
            var deletedPropRDFTriples = comparison
                .OnlyIn1
                .Select(existingPropRdfTriple => WriteRDFTriple.Create(new DeleteRDFTriple(
                    options.TableName,
                    existingPropRdfTriple.Value.AsKey(),
                    VersionDetailCondition.None)))
                .ToImmutableList();

            // Add property RDFTriple which are in target state and not in current state
            var addedPropRDFTriples = comparison
                .OnlyIn2
                .Select(v => WriteRDFTriple.Create(new AddRDFTriple(
                    options.TableName,
                    v.Value)))
                .ToImmutableList();

            var updatedRDFTriples = comparison
                .Different
                .Select(v => WriteRDFTriple.Create(new UpdateRDFTriple(
                    options.TableName,
                    v.Value.Item2,
                    VersionDetailCondition.None)))
                .ToImmutableList();

            return hasEdgeRDFTriples
                .AddRange(updatedRDFTriples)
                .AddRange(deletedPropRDFTriples)
                .AddRange(addedPropRDFTriples);
        }

        private ImmutableList<WriteRDFTriple> GetDeleteTransactWriteItems(INode node, bool checkNode, bool checkAllEdges)
        {
            var options = graphSettingsService.GetGraphSettings();

            var deleteHasTypeRDFTriple = WriteRDFTriple.Create(
                new DeleteRDFTriple(
                    options.TableName,
                    rdfTripleFactory.GetHasTypeRDFTriple(node).AsKey(),
                    new VersionDetailCondition(
                        checkNode ? node.Version.NodeVersion - 1 : null,
                        checkAllEdges ? node.Version.AllEdgesVersion : null)));

            var deleteHasPropRDFTriples = rdfTripleFactory
                .GetHasPropRDFTriples(node)
                .Select(rdfTriple => WriteRDFTriple.Create(
                    new DeleteRDFTriple(
                        options.TableName,
                        rdfTriple.AsKey(),
                        VersionDetailCondition.None)))
                .ToImmutableList();

            var addHasBlobRDFTriple = WriteRDFTriple.Create(
                new AddRDFTriple(
                    options.TableName,
                    rdfTripleFactory.GetHasBlobRDFTriple(node)));

            return deleteHasPropRDFTriples
                .Add(deleteHasTypeRDFTriple)
                .Add(addHasBlobRDFTriple);
        }

#pragma warning disable IDE0060 // Remove unused parameter
        private ImmutableList<WriteRDFTriple> GetDeleteTransactWriteItems(IEdge edge, bool checkNode, bool checkAllEdges)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            var options = graphSettingsService.GetGraphSettings();

            // TODO Add a node check ???

            var rdfTriplesToDelete = rdfTripleFactory
                .GetHasEdgeRDFTriples(edge)
                .AddRange(rdfTripleFactory.GetHasEdgePropRDFTriples(edge))
                .Select(rdfTriple => WriteRDFTriple.Create(
                    new DeleteRDFTriple(
                        options.TableName,
                        rdfTriple.AsKey(),
                        VersionDetailCondition.None)))
                .ToImmutableList();

            var rdfTriplesToAdd = rdfTripleFactory
                .GetHadEdgeRDFTriples(edge)
                .Select(rdfTriple => WriteRDFTriple.Create(
                    new AddRDFTriple(
                        options.TableName,
                        rdfTriple)))
                .ToImmutableList();

            return rdfTriplesToDelete
                .AddRange(rdfTriplesToAdd);
        }

        private static int GetLimit(ConnectionArguments? connectionArguments)
        {
            if (connectionArguments == null)
            {
                return 1000;
            }

            return connectionArguments.First ?? connectionArguments.Last ?? throw new GraphlessDBOperationException("Expected First or Last");
        }

        private static bool GetScanIndexForward(ConnectionArguments? connectionArguments, bool orderDesc = false)
        {
            var scanForward = connectionArguments == null || connectionArguments.First != null;
            if (orderDesc)
            {
                scanForward = !scanForward;
            }

            return scanForward;
        }

        private static CursorNode ToHasTypeCursorItem(RDFTriple value, ImmutableList<HasTypeCursorQueryCursor> queryCursors)
        {
            if (!HasType.IsPredicate(value.Predicate))
            {
                throw new GraphlessDBOperationException("Expected HasType RDFTriple");
            }

            return CursorNode.Empty with
            {
                HasType = new HasTypeCursor(value.Subject, value.Partition, queryCursors)
            };
        }

        private static CursorNode ToHasPropCursorItem(RDFTriple value, ImmutableList<HasPropCursorQueryCursor> queryCursors)
        {
            if (!HasProp.IsPredicate(value.Predicate))
            {
                throw new GraphlessDBOperationException("Expected HasProp RDFTriple");
            }

            return CursorNode.Empty with
            {
                HasProp = new HasPropCursor(value.Subject, HasProp.Parse(value.Predicate).PropertyValue, value.Partition, queryCursors)
            };
        }

        private static CursorNode ToCursorItem(RDFTriple value, ImmutableList<RDFTripleQueryPosition> valuesByQuery)
        {
            if (HasType.IsPredicate(value.Predicate))
            {
                return ToHasTypeCursorItem(value, valuesByQuery
                    .Select(v => new HasTypeCursorQueryCursor(v.Index, v.Position, v.RDFTriple != null ? new HasTypeCursorPartitionCursor(v.RDFTriple.Subject, v.RDFTriple.Partition) : null))
                    .ToImmutableList());
            }

            if (HasProp.IsPredicate(value.Predicate))
            {
                return ToHasPropCursorItem(value, valuesByQuery
                    .Select(v => new HasPropCursorQueryCursor(v.Index, v.Position, v.RDFTriple != null ? new HasPropCursorPartitionCursor(v.RDFTriple.Subject, HasProp.Parse(v.RDFTriple.Predicate).PropertyValue, v.RDFTriple.Partition) : null))
                    .ToImmutableList());
            }

            if (HasInEdge.IsPredicate(value.Predicate))
            {
                var predicate = HasInEdge.Parse(value.Predicate);
                return CursorNode.Empty with { HasInEdge = new HasInEdgeCursor(value.Subject, predicate.EdgeTypeName, predicate.NodeOutId) };
            }

            if (HasInEdgeProp.IsPredicate(value.Predicate))
            {
                var predicate = HasInEdgeProp.Parse(value.Predicate);
                return CursorNode.Empty with { HasInEdgeProp = new HasInEdgePropCursor(value.Subject, predicate.EdgeTypeName, predicate.NodeOutId, predicate.PropertyValue) };
            }

            if (HasOutEdge.IsPredicate(value.Predicate))
            {
                var predicate = HasOutEdge.Parse(value.Predicate);
                return CursorNode.Empty with { HasOutEdge = new HasOutEdgeCursor(value.Subject, predicate.EdgeTypeName, predicate.NodeInId) };
            }

            if (HasOutEdgeProp.IsPredicate(value.Predicate))
            {
                var predicate = HasOutEdgeProp.Parse(value.Predicate);
                return CursorNode.Empty with { HasOutEdgeProp = new HasOutEdgePropCursor(value.Subject, predicate.EdgeTypeName, predicate.NodeInId, predicate.PropertyValue) };
            }

            throw new NotSupportedException();
        }
    }
}