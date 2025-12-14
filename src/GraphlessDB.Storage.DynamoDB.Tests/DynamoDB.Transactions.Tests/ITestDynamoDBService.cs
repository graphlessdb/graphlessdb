/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    public interface ITestDynamoDBService
    {
        string GetTableName();

        ImmutableDictionary<string, AttributeValue> NewKey();
    }
}
