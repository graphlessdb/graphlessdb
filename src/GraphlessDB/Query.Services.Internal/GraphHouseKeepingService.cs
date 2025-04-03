/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Storage.Services;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class GraphHouseKeepingService(IRDFTripleStore rdfTripleStore) : IGraphHouseKeepingService
    {
        public async Task RunHouseKeepingAsync(CancellationToken cancellationToken)
        {
            await rdfTripleStore.RunHouseKeepingAsync(cancellationToken);
        }
    }
}
