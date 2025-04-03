/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.Storage.Services.DynamoDB
{
    public interface IAmazonDynamoDBRDFTripleItemService
    {
        Dictionary<string, AttributeValue> ToAttributeMap(RDFTripleKey key);

        Dictionary<string, AttributeValue> ToAttributeMap(RDFTripleKeyWithPartition key);

        Dictionary<string, AttributeValue> ToAttributeMap(RDFTriple value);

        bool IsRDFTriple(Dictionary<string, AttributeValue> value);

        RDFTriple ToRDFTriple(Dictionary<string, AttributeValue> value);
    }
}