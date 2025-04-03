/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    public sealed record RequestRecord(
        int Id,
        GetItemRequest? GetItemRequest,
        PutItemRequest? PutItemRequest,
        UpdateItemRequest? UpdateItemRequest,
        DeleteItemRequest? DeleteItemRequest,
        TransactGetItemsRequest? TransactGetItemsRequest,
        TransactWriteItemsRequest? TransactWriteItemsRequest)
    {
        public static RequestRecord Create(int id, AmazonDynamoDBRequest request)
        {
            if (request is GetItemRequest getItemRequest)
            {
                return new RequestRecord(id, getItemRequest, null, null, null, null, null);
            }

            if (request is PutItemRequest putItemRequest)
            {
                return new RequestRecord(id, null, putItemRequest, null, null, null, null);
            }

            if (request is UpdateItemRequest updateItemRequest)
            {
                return new RequestRecord(id, null, null, updateItemRequest, null, null, null);
            }

            if (request is DeleteItemRequest deleteItemRequest)
            {
                return new RequestRecord(id, null, null, null, deleteItemRequest, null, null);
            }

            if (request is TransactGetItemsRequest transactGetItemsRequest)
            {
                return new RequestRecord(id, null, null, null, null, transactGetItemsRequest, null);
            }

            if (request is TransactWriteItemsRequest transactWriteItemsRequest)
            {
                return new RequestRecord(id, null, null, null, null, null, transactWriteItemsRequest);
            }

            throw new NotSupportedException("Request type is not supported");
        }

        public AmazonDynamoDBRequest GetRequest()
        {
            if (GetItemRequest != null)
            {
                return GetItemRequest;
            }

            if (PutItemRequest != null)
            {
                return PutItemRequest;
            }

            if (UpdateItemRequest != null)
            {
                return UpdateItemRequest;
            }

            if (DeleteItemRequest != null)
            {
                return DeleteItemRequest;
            }

            if (TransactGetItemsRequest != null)
            {
                return TransactGetItemsRequest;
            }

            if (TransactWriteItemsRequest != null)
            {
                return TransactWriteItemsRequest;
            }

            throw new InvalidOperationException();
        }
    }
}