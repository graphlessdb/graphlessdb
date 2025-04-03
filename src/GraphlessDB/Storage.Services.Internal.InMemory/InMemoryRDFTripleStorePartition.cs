/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Generic;

namespace GraphlessDB.Storage.Services.Internal.InMemory
{
    internal sealed record InMemoryRDFTripleStorePartition(
        InMemoryRDFTripleStoreIndex PredicatesBySubject,
        Dictionary<RDFTripleKey, RDFTriple> ItemsByKey)
    {
        public static InMemoryRDFTripleStorePartition Create()
        {
            return new InMemoryRDFTripleStorePartition(new InMemoryRDFTripleStoreIndex([], []), []);
        }
    }
}
