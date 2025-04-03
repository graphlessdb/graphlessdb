/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace GraphlessDB.Extensions.DependencyInjection
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddTestInstrumentation(this IServiceCollection source, bool useDebugForLogging)
        {
            return source
                .AddLogging(options =>
                {
                    if (useDebugForLogging)
                    {
                        options.ClearProviders().AddDebug();
                    }
                    else
                    {
                        options.ClearProviders().AddConsole();
                    }
                });
        }
    }
}
