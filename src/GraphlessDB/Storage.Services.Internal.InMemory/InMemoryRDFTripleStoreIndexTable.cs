/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Generic;
using System.Linq;

namespace GraphlessDB.Storage.Services.Internal.InMemory
{
    internal sealed record InMemoryRDFTripleStoreIndexTable(
        string IndexName,
        List<InMemoryRDFTripleStoreIndex> Partitions)
    {
        public static InMemoryRDFTripleStoreIndexTable Create(string name, int partitionCount)
        {
            return new InMemoryRDFTripleStoreIndexTable(
                name,
                Enumerable
                    .Range(0, partitionCount)
                    .Select(i => InMemoryRDFTripleStoreIndex.Create())
                    .ToList());
        }
    }
}
