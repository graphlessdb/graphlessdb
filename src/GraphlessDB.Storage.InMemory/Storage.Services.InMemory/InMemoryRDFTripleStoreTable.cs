/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Generic;
using System.Linq;
using GraphlessDB.Domain.Graph;

namespace GraphlessDB.Storage.Services.InMemory
{
    internal sealed record InMemoryRDFTripleStoreTable(
        string TableName,
        List<InMemoryRDFTripleStorePartition> Partitions)
    {
        public static InMemoryRDFTripleStoreTable Create(string name, int partitionCount)
        {
            return new InMemoryRDFTripleStoreTable(
                name,
                Enumerable
                    .Range(0, partitionCount)
                    .Select(i => InMemoryRDFTripleStorePartition.Create())
                    .ToList());
        }
    }
}
