/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB.Query.Services.Internal
{
    internal static class EdgeFilterServiceExtensions
    {
        public static async Task<Connection<RelayEdge<IEdge>, IEdge>> GetFilteredEdgeConnectionAsync(
            this IGraphEdgeFilterService source,
            Connection<RelayEdge<IEdge>, IEdge> connection,
            IEdgeFilter? filter,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            var edgeFilter = filter?.GetEdgeFilter();
            if (edgeFilter != null && (edgeFilter.ValueFilterItems.Count > 1 || edgeFilter.NodeInFilter != null || edgeFilter.NodeOutFilter != null))
            {
                throw new NotSupportedException("EdgeFilter not supported");
            }

            return connection;
            // var filteredEdges = await Task.WhenAll(connection
            //     .Edges
            //     .Select(async edge =>
            //     {
            //         return new
            //         {
            //             edge,
            //             match = await source.IsFilterMatchAsync(edge.Node, filter, consistentRead, cancellationToken)
            //         };
            //     }));

            // var edges = filteredEdges
            //     .Where(e => e.match)
            //     .Select(e => e.edge)
            //     .ToImmutableList();

            // return new Connection<RelayEdge<IEdge>, IEdge>(
            //     edges,
            //     new PageInfo(
            //         connection.Edges.Count > edges.Count || connection.PageInfo.HasNextPage,
            //         connection.PageInfo.HasPreviousPage,
            //         edges.Select(e => e.Cursor).FirstOrDefault() ?? connection.PageInfo.StartCursor,
            //         edges.Select(e => e.Cursor).LastOrDefault() ?? connection.PageInfo.EndCursor));
        }
    }
}
