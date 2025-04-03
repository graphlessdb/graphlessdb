/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB.Query.Services
{
    public interface IGraphEdgeFilterService
    {
        bool IsPostFilteringRequired(
            IEdgeFilter? filter);

        EdgePushdownQueryData? TryGetEdgePushdownQueryData(
            string? edgeTypeName,
            IEdgeFilter? filter,
            IEdgeOrder? order);

        Task<bool> IsFilterMatchAsync(
              IEdge edge,
              IEdgeFilter? filter,
              bool consistentRead,
              CancellationToken cancellationToken);
    }
}
