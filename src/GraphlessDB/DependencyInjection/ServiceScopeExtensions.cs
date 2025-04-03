/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Storage.Services.Internal.InMemory;
using Microsoft.Extensions.DependencyInjection;

namespace GraphlessDB.DependencyInjection
{
    internal static class ServiceScopeExtensions
    {
        public static async Task ProcessInMemoryNodeEventsAsync(
            this IServiceScope source,
            CancellationToken cancellationToken)
        {
            await source
                .ServiceProvider
                .GetRequiredService<IInMemoryNodeEventProcessor>()
                .ProcessInMemoryNodeEventsAsync(cancellationToken);
        }
    }
}
