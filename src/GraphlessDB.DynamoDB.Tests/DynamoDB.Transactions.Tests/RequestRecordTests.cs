/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    [TestClass]
    public sealed class RequestRecordTests
    {
        [TestMethod]
        public void CreateWithGetItemRequestReturnsCorrectRecord()
        {
            var getItemRequest = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "id", new AttributeValue { S = "123" } }
                }
            };

            var record = RequestRecord.Create(1, getItemRequest);

            Assert.AreEqual(1, record.Id);
            Assert.IsNotNull(record.GetItemRequest);
            Assert.AreSame(getItemRequest, record.GetItemRequest);
            Assert.IsNull(record.PutItemRequest);
            Assert.IsNull(record.UpdateItemRequest);
            Assert.IsNull(record.DeleteItemRequest);
            Assert.IsNull(record.TransactGetItemsRequest);
            Assert.IsNull(record.TransactWriteItemsRequest);
        }

        [TestMethod]
        public void CreateWithPutItemRequestReturnsCorrectRecord()
        {
            var putItemRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    { "id", new AttributeValue { S = "123" } }
                }
            };

            var record = RequestRecord.Create(2, putItemRequest);

            Assert.AreEqual(2, record.Id);
            Assert.IsNull(record.GetItemRequest);
            Assert.IsNotNull(record.PutItemRequest);
            Assert.AreSame(putItemRequest, record.PutItemRequest);
            Assert.IsNull(record.UpdateItemRequest);
            Assert.IsNull(record.DeleteItemRequest);
            Assert.IsNull(record.TransactGetItemsRequest);
            Assert.IsNull(record.TransactWriteItemsRequest);
        }

        [TestMethod]
        public void CreateWithUpdateItemRequestReturnsCorrectRecord()
        {
            var updateItemRequest = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "id", new AttributeValue { S = "123" } }
                }
            };

            var record = RequestRecord.Create(3, updateItemRequest);

            Assert.AreEqual(3, record.Id);
            Assert.IsNull(record.GetItemRequest);
            Assert.IsNull(record.PutItemRequest);
            Assert.IsNotNull(record.UpdateItemRequest);
            Assert.AreSame(updateItemRequest, record.UpdateItemRequest);
            Assert.IsNull(record.DeleteItemRequest);
            Assert.IsNull(record.TransactGetItemsRequest);
            Assert.IsNull(record.TransactWriteItemsRequest);
        }

        [TestMethod]
        public void CreateWithDeleteItemRequestReturnsCorrectRecord()
        {
            var deleteItemRequest = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "id", new AttributeValue { S = "123" } }
                }
            };

            var record = RequestRecord.Create(4, deleteItemRequest);

            Assert.AreEqual(4, record.Id);
            Assert.IsNull(record.GetItemRequest);
            Assert.IsNull(record.PutItemRequest);
            Assert.IsNull(record.UpdateItemRequest);
            Assert.IsNotNull(record.DeleteItemRequest);
            Assert.AreSame(deleteItemRequest, record.DeleteItemRequest);
            Assert.IsNull(record.TransactGetItemsRequest);
            Assert.IsNull(record.TransactWriteItemsRequest);
        }

        [TestMethod]
        public void CreateWithTransactGetItemsRequestReturnsCorrectRecord()
        {
            var transactGetItemsRequest = new TransactGetItemsRequest
            {
                TransactItems = new System.Collections.Generic.List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "id", new AttributeValue { S = "123" } }
                            }
                        }
                    }
                }
            };

            var record = RequestRecord.Create(5, transactGetItemsRequest);

            Assert.AreEqual(5, record.Id);
            Assert.IsNull(record.GetItemRequest);
            Assert.IsNull(record.PutItemRequest);
            Assert.IsNull(record.UpdateItemRequest);
            Assert.IsNull(record.DeleteItemRequest);
            Assert.IsNotNull(record.TransactGetItemsRequest);
            Assert.AreSame(transactGetItemsRequest, record.TransactGetItemsRequest);
            Assert.IsNull(record.TransactWriteItemsRequest);
        }

        [TestMethod]
        public void CreateWithTransactWriteItemsRequestReturnsCorrectRecord()
        {
            var transactWriteItemsRequest = new TransactWriteItemsRequest
            {
                TransactItems = new System.Collections.Generic.List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "id", new AttributeValue { S = "123" } }
                            }
                        }
                    }
                }
            };

            var record = RequestRecord.Create(6, transactWriteItemsRequest);

            Assert.AreEqual(6, record.Id);
            Assert.IsNull(record.GetItemRequest);
            Assert.IsNull(record.PutItemRequest);
            Assert.IsNull(record.UpdateItemRequest);
            Assert.IsNull(record.DeleteItemRequest);
            Assert.IsNull(record.TransactGetItemsRequest);
            Assert.IsNotNull(record.TransactWriteItemsRequest);
            Assert.AreSame(transactWriteItemsRequest, record.TransactWriteItemsRequest);
        }

        [TestMethod]
        public void CreateWithUnsupportedRequestTypeThrowsNotSupportedException()
        {
            var unsupportedRequest = new CreateTableRequest { TableName = "TestTable" };

            Assert.ThrowsException<NotSupportedException>(() => RequestRecord.Create(7, unsupportedRequest));
        }

        [TestMethod]
        public void GetRequestReturnsGetItemRequest()
        {
            var getItemRequest = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "id", new AttributeValue { S = "123" } }
                }
            };
            var record = RequestRecord.Create(1, getItemRequest);

            var result = record.GetRequest();

            Assert.IsNotNull(result);
            Assert.AreSame(getItemRequest, result);
        }

        [TestMethod]
        public void GetRequestReturnsPutItemRequest()
        {
            var putItemRequest = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>
                {
                    { "id", new AttributeValue { S = "123" } }
                }
            };
            var record = RequestRecord.Create(2, putItemRequest);

            var result = record.GetRequest();

            Assert.IsNotNull(result);
            Assert.AreSame(putItemRequest, result);
        }

        [TestMethod]
        public void GetRequestReturnsUpdateItemRequest()
        {
            var updateItemRequest = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "id", new AttributeValue { S = "123" } }
                }
            };
            var record = RequestRecord.Create(3, updateItemRequest);

            var result = record.GetRequest();

            Assert.IsNotNull(result);
            Assert.AreSame(updateItemRequest, result);
        }

        [TestMethod]
        public void GetRequestReturnsDeleteItemRequest()
        {
            var deleteItemRequest = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "id", new AttributeValue { S = "123" } }
                }
            };
            var record = RequestRecord.Create(4, deleteItemRequest);

            var result = record.GetRequest();

            Assert.IsNotNull(result);
            Assert.AreSame(deleteItemRequest, result);
        }

        [TestMethod]
        public void GetRequestReturnsTransactGetItemsRequest()
        {
            var transactGetItemsRequest = new TransactGetItemsRequest
            {
                TransactItems = new System.Collections.Generic.List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "id", new AttributeValue { S = "123" } }
                            }
                        }
                    }
                }
            };
            var record = RequestRecord.Create(5, transactGetItemsRequest);

            var result = record.GetRequest();

            Assert.IsNotNull(result);
            Assert.AreSame(transactGetItemsRequest, result);
        }

        [TestMethod]
        public void GetRequestReturnsTransactWriteItemsRequest()
        {
            var transactWriteItemsRequest = new TransactWriteItemsRequest
            {
                TransactItems = new System.Collections.Generic.List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = "TestTable",
                            Item = new Dictionary<string, AttributeValue>
                            {
                                { "id", new AttributeValue { S = "123" } }
                            }
                        }
                    }
                }
            };
            var record = RequestRecord.Create(6, transactWriteItemsRequest);

            var result = record.GetRequest();

            Assert.IsNotNull(result);
            Assert.AreSame(transactWriteItemsRequest, result);
        }

        [TestMethod]
        public void GetRequestThrowsInvalidOperationExceptionWhenNoRequestIsSet()
        {
            var record = new RequestRecord(7, null, null, null, null, null, null);

            Assert.ThrowsException<InvalidOperationException>(() => record.GetRequest());
        }
    }
}
