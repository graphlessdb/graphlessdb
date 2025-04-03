/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB.Storage.Services
{
    public interface IRDFTripleKeyValueStore
    {
        Task<GetRDFTriplesResponse> GetRDFTriplesAsync(GetRDFTriplesRequest request, CancellationToken cancellationToken);

        Task<WriteRDFTriplesResponse> WriteRDFTriplesAsync(WriteRDFTriplesRequest request, CancellationToken cancellationToken);

        Task RunHouseKeepingAsync(CancellationToken cancellationToken);
    }
}
