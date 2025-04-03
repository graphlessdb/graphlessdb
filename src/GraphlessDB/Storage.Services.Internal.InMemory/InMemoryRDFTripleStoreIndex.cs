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
    internal sealed record InMemoryRDFTripleStoreIndex(
        Dictionary<string, List<string>> SortKeysByPartitionKey,
        Dictionary<string, List<RDFTripleKey>> RDFTripleKeysByPartitionKey)
    {
        public static InMemoryRDFTripleStoreIndex Create()
        {
            return new InMemoryRDFTripleStoreIndex([], []);
        }
    }
}
