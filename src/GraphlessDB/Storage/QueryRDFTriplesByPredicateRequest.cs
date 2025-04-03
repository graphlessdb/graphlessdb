/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Storage
{
    public sealed record QueryRDFTriplesByPartitionAndPredicateRequest(
        string TableName,
        string Partition,
        string PredicateBeginsWith,
        RDFTripleKeyWithPartition? ExclusiveStartKey,
        bool ScanIndexForward,
        int Limit,
        bool ConsistentRead,
        bool DisableInconsistentCacheRead);
}
