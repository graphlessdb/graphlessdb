/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace GraphlessDB
{
    public static class ServiceScopeExtensions
    {
        public static Task ProcessInMemoryNodeEventsAsync(
            this IServiceScope source,
            CancellationToken cancellationToken)
        {
            return DependencyInjection.ServiceScopeExtensions.ProcessInMemoryNodeEventsAsync(source, cancellationToken);
        }

        public static IGraphDB GraphDB(this IServiceScope source)
        {
            return source.ServiceProvider.GetRequiredService<IGraphDB>();
        }
    }
}
