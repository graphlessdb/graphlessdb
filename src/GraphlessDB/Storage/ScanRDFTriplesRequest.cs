/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Storage
{
    public sealed record ScanRDFTriplesRequest(
        string TableName,
        RDFTripleKey? ExclusiveStartKey,
        int Limit,
        bool ConsistentRead,
        bool DisableInconsistentCacheRead);
}
