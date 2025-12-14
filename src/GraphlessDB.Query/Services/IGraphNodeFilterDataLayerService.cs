/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Domain;
using GraphlessDB.Domain.Graph;
using GraphlessDB.Domain.Services;
using GraphlessDB;

namespace GraphlessDB.Query.Services
{
    public interface IGraphNodeFilterDataLayerService
    {
        INodeFilter? TryGetDataLayerFilter(INodeFilter? filter, CancellationToken cancellationToken);

        Task<bool> IsNodeExcludedAsync(INode node, INodeFilter filter, CancellationToken cancellationToken);
    }
}
