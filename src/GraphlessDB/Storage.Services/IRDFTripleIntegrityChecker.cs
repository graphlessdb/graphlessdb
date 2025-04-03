/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB.Storage.Services
{
    public interface IRDFTripleIntegrityChecker
    {
        Task<RDFTripleIntegrityReport> CheckIntegrityAsync(CancellationToken cancellationToken);

        Task ClearAllDataAsync(CancellationToken cancellationToken);

        Task RemoveRdfTriplesAsync(ImmutableList<RDFTriple> values, CancellationToken cancellationToken);
    }
}