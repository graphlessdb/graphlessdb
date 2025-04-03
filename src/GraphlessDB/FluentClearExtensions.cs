/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB
{
    public static class FluentClearExtensions
    {
        public static async Task ExecuteAsync(this FluentClear source, CancellationToken cancellationToken)
        {
            await source.GraphQueryService.ClearAsync(cancellationToken);
        }
    }
}
