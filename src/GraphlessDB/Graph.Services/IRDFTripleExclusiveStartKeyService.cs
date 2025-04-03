/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Storage;

namespace GraphlessDB.Graph.Services
{
    public interface IRDFTripleExclusiveStartKeyService
    {
        HasTypeCursor? TryGetHasTypeCursor(ConnectionArguments page);

        HasPropCursor? TryGetHasPropCursor(ConnectionArguments page);

        RDFTripleKeyWithPartition? TryGetHasTypeExclusiveStartKey(ConnectionArguments? page, int queryIndex, string typeName);

        RDFTripleKey? TryGetHasEdgeExclusiveStartKey(ConnectionArguments? connectionArguments, GraphSettings options, ToEdgeQueryRequest request);

        RDFTripleKeyWithPartition? TryGetPropertiesByTypeAndPropertyNameExclusiveStartKey(ConnectionArguments page, int queryIndex, string typeName, string propertyName);

        RDFTripleKeyWithPartition? TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKey(ConnectionArguments page, int queryIndex, string typeName, string propertyName);
    }
}