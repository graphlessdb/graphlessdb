/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;
using GraphlessDB.Graph.Services.Internal;
using GraphlessDB.Linq;

namespace GraphlessDB.Graph.Services
{
    internal static class GraphQueryServiceExtensions
    {
        public static async Task<GetNodesResponse> GetNodesAsync(
            this IGraphQueryService source,
            GetNodesRequest request,
            CancellationToken cancellationToken)
        {
            var tryGetNodesResponse = await source.TryGetNodesAsync(
                new TryGetNodesRequest(request.Ids, request.ConsistentRead),
                cancellationToken);

            return new GetNodesResponse(tryGetNodesResponse
                .Nodes
                .Select(n => new RelayEdge<INode>(
                    n?.Cursor ?? throw new NodesNotFoundException(),
                    n?.Node ?? throw new NodesNotFoundException()))
                .ToImmutableList());
        }

        public static async Task<GetVersionedNodesResponse> GetVersionedNodesAsync(
            this IGraphQueryService source,
            GetVersionedNodesRequest request,
            CancellationToken cancellationToken)
        {
            var response = await source.TryGetVersionedNodesAsync(
                new TryGetVersionedNodesRequest(request.Keys, request.ConsistentRead),
                cancellationToken);

            if (response.Nodes.Any(node => node == null))
            {
                throw new NodesNotFoundException();
            }

            return new GetVersionedNodesResponse(response
                .Nodes
                .Select(n => new RelayEdge<INode>(
                    n?.Cursor ?? throw new NodesNotFoundException(),
                    n?.Node ?? throw new NodesNotFoundException()))
                .ToImmutableList());
        }

        public static async Task<RelayEdge<INode>?> TryGetNodeAsync(
            this IGraphQueryService source,
            string id,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var response = await source.TryGetNodesAsync(
                new TryGetNodesRequest([id], consistentRead), cancellationToken);

            return response.Nodes.SingleOrDefault();
        }

        public static async Task<RelayEdge<INode>?> TryGetVersionedNodeAsync(
            this IGraphQueryService source,
            VersionedNodeKey key,
            CancellationToken cancellationToken)
        {
            var response = await source.TryGetVersionedNodesAsync(
                new TryGetVersionedNodesRequest([key], false), cancellationToken);

            return response.Nodes.SingleOrDefault();
        }


        public static async Task<RelayEdge<INode>?> TryGetVersionedNodeAsync(
            this IGraphQueryService source,
            VersionedNodeKey key,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var response = await source.TryGetVersionedNodesAsync(
                new TryGetVersionedNodesRequest([key], consistentRead), cancellationToken);

            return response.Nodes.SingleOrDefault();
        }

        public static async Task<RelayEdge<IEdge>> GetEdgeAsync(
            this IGraphQueryService source,
            EdgeKey edgeKey,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var response = await source.GetEdgesAsync(
                new GetEdgesRequest([edgeKey], consistentRead),
                cancellationToken);

            return response.Edges.Single();
        }

        public static async Task<GetEdgesResponse> GetEdgesAsync(
            this IGraphQueryService source,
            GetEdgesRequest request,
            CancellationToken cancellationToken)
        {
            var response = await source.TryGetEdgesAsync(
                new TryGetEdgesRequest(request.Keys, request.ConsistentRead),
                cancellationToken);

            var missingEdgeKeys = response
                .Edges
                .Select((edge, i) => new { edge, i })
                .Where(v => v.edge == null)
                .Select(v => request.Keys[v.i])
                .NotNull()
                .ToImmutableList();

            if (!missingEdgeKeys.IsEmpty)
            {
                throw new EdgesNotFoundException(missingEdgeKeys);
            }

            return new GetEdgesResponse(
                response.Edges.Cast<RelayEdge<IEdge>>().ToImmutableList());
        }

        public static async Task<RelayEdge<IEdge>?> TryGetEdgeAsync(
            this IGraphQueryService source,
            EdgeKey key,
            CancellationToken cancellationToken)
        {
            return await source.TryGetEdgeAsync(key, false, cancellationToken);
        }

        public static async Task<RelayEdge<IEdge>?> TryGetEdgeAsync(
            this IGraphQueryService source,
            EdgeKey key,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var response = await source.TryGetEdgesAsync(
                new TryGetEdgesRequest([key], consistentRead),
                cancellationToken);

            return response.Edges.SingleOrDefault();
        }

        public static async Task<RelayEdge<INode>> GetNodeAsync(
            this IGraphQueryService source,
            string id,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var response = await source.TryGetNodesAsync(
                new TryGetNodesRequest([id], consistentRead), cancellationToken);

            return response.Nodes.Single() ?? throw new NodesNotFoundException([id]);
        }

        public static async Task<RelayEdge<INode>> GetVersionedNodeAsync(
            this IGraphQueryService source,
            VersionedNodeKey key,
            CancellationToken cancellationToken)
        {
            return await source.GetVersionedNodeAsync(key, false, cancellationToken);
        }

        public static async Task<RelayEdge<INode>> GetVersionedNodeAsync(
            this IGraphQueryService source,
            VersionedNodeKey key,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var response = await source.TryGetVersionedNodesAsync(
                new TryGetVersionedNodesRequest([key], consistentRead),
                cancellationToken);

            return response.Nodes.Single() ?? throw new NodesNotFoundException();
        }
    }
}
