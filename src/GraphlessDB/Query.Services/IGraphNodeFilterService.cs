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
    public interface IGraphNodeFilterService
    {
        bool IsPostFilteringRequired(
            INodeFilter? filter);

        NodePushdownQueryData? TryGetNodePushdownQueryData(
            string type,
            INodeFilter? filter,
            INodeOrder? order,
            CancellationToken cancellationToken);

        Task<bool> IsFilterMatchAsync(
            INode node,
            INodeFilter? filter,
            bool consistentRead,
            CancellationToken cancellationToken);
    }
}
