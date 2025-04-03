/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class EmptyGraphNodeFilterDataLayerService : IGraphNodeFilterDataLayerService
    {
        public Task<bool> IsNodeExcludedAsync(INode node, INodeFilter filter, CancellationToken cancellationToken)
        {
            return Task.FromResult(false);
        }

        public INodeFilter? TryGetDataLayerFilter(INodeFilter? filter, CancellationToken cancellationToken)
        {
            return filter;
        }
    }
}
