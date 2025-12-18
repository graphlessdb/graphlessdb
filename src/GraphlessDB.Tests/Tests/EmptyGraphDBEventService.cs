/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Domain.Graph;
using GraphlessDB.Domain.Graph.Services;

namespace GraphlessDB.Tests
{
    public sealed class EmptyGraphDBEventService : IGraphEventService
    {
        public Task OnNodeEventAsync(NodeEvent nodeEvent, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
