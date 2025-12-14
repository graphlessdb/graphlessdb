/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Tests
{
    [TestClass]
    public sealed class AmazonDynamoDBRequestExtensionsTests
    {
        [TestMethod]
        public void GetTableNameReturnsTableNameForGetItemRequest()
        {
            var request = new GetItemRequest { TableName = "TestTable" };
            var result = request.GetTableName();
            Assert.AreEqual("TestTable", result);
        }

        [TestMethod]
        public void GetTableNameReturnsTableNameForPutItemRequest()
        {
            var request = new PutItemRequest { TableName = "TestTable" };
            var result = request.GetTableName();
            Assert.AreEqual("TestTable", result);
        }

        [TestMethod]
        public void GetTableNameReturnsTableNameForDeleteItemRequest()
        {
            var request = new DeleteItemRequest { TableName = "TestTable" };
            var result = request.GetTableName();
            Assert.AreEqual("TestTable", result);
        }

        [TestMethod]
        public void GetTableNameReturnsTableNameForUpdateItemRequest()
        {
            var request = new UpdateItemRequest { TableName = "TestTable" };
            var result = request.GetTableName();
            Assert.AreEqual("TestTable", result);
        }

        [TestMethod]
        public void GetTableNameReturnsTableNameForTransactWriteItemsRequestWithPutItem()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put { TableName = "TestTable" }
                    }
                }
            };
            var result = request.GetTableName();
            Assert.AreEqual("TestTable", result);
        }

        [TestMethod]
        public void GetTableNameReturnsTableNameForTransactWriteItemsRequestWithUpdateItem()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Update = new Update { TableName = "TestTable" }
                    }
                }
            };
            var result = request.GetTableName();
            Assert.AreEqual("TestTable", result);
        }

        [TestMethod]
        public void GetTableNameReturnsTableNameForTransactWriteItemsRequestWithDeleteItem()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Delete = new Delete { TableName = "TestTable" }
                    }
                }
            };
            var result = request.GetTableName();
            Assert.AreEqual("TestTable", result);
        }

        [TestMethod]
        public void GetTableNameReturnsTableNameForTransactWriteItemsRequestWithConditionCheck()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        ConditionCheck = new ConditionCheck { TableName = "TestTable" }
                    }
                }
            };
            var result = request.GetTableName();
            Assert.AreEqual("TestTable", result);
        }

        [TestMethod]
        public void GetTableNameReturnsTableNameForTransactWriteItemsRequestWithMultipleItemsSameTable()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put { TableName = "TestTable" }
                    },
                    new TransactWriteItem
                    {
                        Update = new Update { TableName = "TestTable" }
                    },
                    new TransactWriteItem
                    {
                        Delete = new Delete { TableName = "TestTable" }
                    }
                }
            };
            var result = request.GetTableName();
            Assert.AreEqual("TestTable", result);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void GetTableNameThrowsNotSupportedExceptionForUnsupportedRequestType()
        {
            var request = new QueryRequest { TableName = "TestTable" };
            request.GetTableName();
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void GetTableNameThrowsNotSupportedExceptionForTransactWriteItemWithNoOperation()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem()
                }
            };
            request.GetTableName();
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetTableNameThrowsInvalidOperationExceptionForTransactWriteItemsWithMultipleTables()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put { TableName = "TestTable1" }
                    },
                    new TransactWriteItem
                    {
                        Put = new Put { TableName = "TestTable2" }
                    }
                }
            };
            request.GetTableName();
        }

        [TestMethod]
        public void GetReturnValuesReturnsReturnValueForPutItemRequest()
        {
            var request = new PutItemRequest { ReturnValues = ReturnValue.ALL_OLD };
            var result = request.GetReturnValues();
            Assert.AreEqual(ReturnValue.ALL_OLD, result);
        }

        [TestMethod]
        public void GetReturnValuesReturnsReturnValueForDeleteItemRequest()
        {
            var request = new DeleteItemRequest { ReturnValues = ReturnValue.ALL_OLD };
            var result = request.GetReturnValues();
            Assert.AreEqual(ReturnValue.ALL_OLD, result);
        }

        [TestMethod]
        public void GetReturnValuesReturnsReturnValueForUpdateItemRequest()
        {
            var request = new UpdateItemRequest { ReturnValues = ReturnValue.ALL_NEW };
            var result = request.GetReturnValues();
            Assert.AreEqual(ReturnValue.ALL_NEW, result);
        }

        [TestMethod]
        public void GetReturnValuesReturnsNullForGetItemRequest()
        {
            var request = new GetItemRequest { TableName = "TestTable" };
            var result = request.GetReturnValues();
            Assert.IsNull(result);
        }

        [TestMethod]
        public void GetReturnValuesReturnsNullForTransactWriteItemsRequest()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put { TableName = "TestTable" }
                    }
                }
            };
            var result = request.GetReturnValues();
            Assert.IsNull(result);
        }

        [TestMethod]
        public void GetReturnValuesReturnsNullForUnsupportedRequestType()
        {
            var request = new QueryRequest { TableName = "TestTable" };
            var result = request.GetReturnValues();
            Assert.IsNull(result);
        }
    }
}
