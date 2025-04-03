/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Linq;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB
{
    public static class RequestExtensions
    {
        public static string GetTableName(this AmazonDynamoDBRequest source)
        {
            if (source is GetItemRequest getItemRequest)
            {
                return getItemRequest.TableName;
            }

            if (source is PutItemRequest putItemRequest)
            {
                return putItemRequest.TableName;
            }

            if (source is DeleteItemRequest deleteItemRequest)
            {
                return deleteItemRequest.TableName;
            }

            if (source is UpdateItemRequest updateItemRequest)
            {
                return updateItemRequest.TableName;
            }

            if (source is TransactWriteItemsRequest transactWriteItemsRequest)
            {
                return transactWriteItemsRequest.TransactItems.Select(GetTableName).Distinct().Single();
            }

            throw new NotSupportedException("Request type is not supported");
        }

        private static string GetTableName(TransactWriteItem item)
        {
            return item.Put?.TableName ??
                item.Update?.TableName ??
                item.Delete?.TableName ??
                item.ConditionCheck?.TableName ??
                throw new NotSupportedException("Request type is not supported");
        }

        public static ReturnValue? GetReturnValues(this AmazonDynamoDBRequest source)
        {
            if (source is PutItemRequest putItemRequest)
            {
                return putItemRequest.ReturnValues;
            }

            if (source is DeleteItemRequest deleteItemRequest)
            {
                return deleteItemRequest.ReturnValues;
            }

            if (source is UpdateItemRequest updateItemRequest)
            {
                return updateItemRequest.ReturnValues;
            }

            return null;
        }
    }
}
