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
    public interface IRDFTripleStore<T> : IRDFTripleStore
        where T : StoreType
    {
    }

    public interface IRDFTripleStore : IRDFTripleKeyValueStore
    {
        Task<ScanRDFTriplesResponse> ScanRDFTriplesAsync(
            ScanRDFTriplesRequest request, CancellationToken cancellationToken);

        Task<QueryRDFTriplesResponse> QueryRDFTriplesAsync(
            QueryRDFTriplesRequest request, CancellationToken cancellationToken);

        Task<QueryRDFTriplesResponse> QueryRDFTriplesByPartitionAndPredicateAsync(
            QueryRDFTriplesByPartitionAndPredicateRequest request, CancellationToken cancellationToken);
    }
}
