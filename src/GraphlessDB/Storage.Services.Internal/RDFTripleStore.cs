/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;

namespace GraphlessDB.Storage.Services.Internal
{
    internal sealed class RDFTripleStore(
        IOptionsSnapshot<RDFTripleStoreOptions> options,
        IRDFTripleStore<StoreType.Cached> rdfTripleStoreCached,
        IRDFTripleStore<StoreType.Data> rdfTripleStoreData) : IRDFTripleStore
    {
        public async Task<GetRDFTriplesResponse> GetRDFTriplesAsync(GetRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            return await GetRDFTripleStore().GetRDFTriplesAsync(request, cancellationToken);
        }

        public async Task<QueryRDFTriplesResponse> QueryRDFTriplesAsync(QueryRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            return await GetRDFTripleStore().QueryRDFTriplesAsync(request, cancellationToken);
        }

        public async Task<QueryRDFTriplesResponse> QueryRDFTriplesByPartitionAndPredicateAsync(QueryRDFTriplesByPartitionAndPredicateRequest request, CancellationToken cancellationToken)
        {
            return await GetRDFTripleStore().QueryRDFTriplesByPartitionAndPredicateAsync(request, cancellationToken);
        }

        public async Task RunHouseKeepingAsync(CancellationToken cancellationToken)
        {
            await GetRDFTripleStore().RunHouseKeepingAsync(cancellationToken);
        }

        public async Task<ScanRDFTriplesResponse> ScanRDFTriplesAsync(ScanRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            return await GetRDFTripleStore().ScanRDFTriplesAsync(request, cancellationToken);
        }

        public async Task<WriteRDFTriplesResponse> WriteRDFTriplesAsync(WriteRDFTriplesRequest request, CancellationToken cancellationToken)
        {
            return await GetRDFTripleStore().WriteRDFTriplesAsync(request, cancellationToken);
        }

        private IRDFTripleStore GetRDFTripleStore()
        {
            return options.Value.ScopeCacheEnabled
                ? rdfTripleStoreCached
                : rdfTripleStoreData;
        }
    }
}
