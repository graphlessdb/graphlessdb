/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Storage.Services.Internal.FileBased;
using GraphlessDB.Storage.Services.Internal.InMemory;
using Microsoft.Extensions.DependencyInjection;

namespace GraphlessDB
{
    public static class ServiceScopeExtensions
    {
        public static IGraphDB GraphDB(this IServiceScope source)
        {
            return source.ServiceProvider.GetRequiredService<IGraphDB>();
        }

        public static async Task ProcessInMemoryNodeEventsAsync(
            this IServiceScope source,
            CancellationToken cancellationToken)
        {
            await source
                .ServiceProvider
                .GetRequiredService<IInMemoryNodeEventProcessor>()
                .ProcessInMemoryNodeEventsAsync(cancellationToken);
        }

        public static async Task ProcessFileBasedNodeEventsAsync(
            this IServiceScope source,
            CancellationToken cancellationToken)
        {
            await source
                .ServiceProvider
                .GetRequiredService<IFileBasedNodeEventProcessor>()
                .ProcessFileBasedNodeEventsAsync(cancellationToken);
        }
    }
}
