/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB.Storage.Services.Internal.InMemory
{
    internal interface IInMemoryNodeEventProcessor
    {
        Task ProcessInMemoryNodeEventsAsync(CancellationToken cancellationToken);
    }
}
