/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.DynamoDB.Transactions.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Internal.Tests
{
    [TestClass]
    public sealed class AmazonDynamoDBWithTransactionsTests
    {
        [TestMethod]
        public void ValidateGetItemRequestThrowsWhenTableNameIsNull()
        {
            var request = new GetItemRequest
            {
                TableName = null,
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                }
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("TableName"));
        }

        [TestMethod]
        public void ValidateGetItemRequestThrowsWhenKeyIsEmpty()
        {
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>()
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("key"));
        }

        [TestMethod]
        public void ValidateGetItemRequestThrowsWhenAttributesToGetIsPopulated()
        {
            var request = new GetItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                },
                AttributesToGet = new List<string> { "attr1" }
            };

            var exception = Assert.ThrowsException<NotSupportedException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("Legacy"));
        }

        [TestMethod]
        public void ValidatePutItemRequestThrowsWhenTableNameIsNull()
        {
            var request = new PutItemRequest
            {
                TableName = null,
                Item = new Dictionary<string, AttributeValue>()
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("TableName"));
        }

        [TestMethod]
        public void ValidatePutItemRequestThrowsWhenExpectedIsPopulated()
        {
            var request = new PutItemRequest
            {
                TableName = "TestTable",
                Item = new Dictionary<string, AttributeValue>(),
                Expected = new Dictionary<string, ExpectedAttributeValue>
                {
                    { "attr1", new ExpectedAttributeValue() }
                }
            };

            var exception = Assert.ThrowsException<NotSupportedException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("Legacy"));
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenTableNameIsNull()
        {
            var request = new UpdateItemRequest
            {
                TableName = null,
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                }
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("TableName"));
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenKeyIsEmpty()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>()
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("key"));
        }

        [TestMethod]
        public void ValidateUpdateItemRequestThrowsWhenAttributeUpdatesIsPopulated()
        {
            var request = new UpdateItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                },
                AttributeUpdates = new Dictionary<string, AttributeValueUpdate>
                {
                    { "attr1", new AttributeValueUpdate() }
                }
            };

            var exception = Assert.ThrowsException<NotSupportedException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("Legacy"));
        }

        [TestMethod]
        public void ValidateDeleteItemRequestThrowsWhenTableNameIsNull()
        {
            var request = new DeleteItemRequest
            {
                TableName = null,
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                }
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("TableName"));
        }

        [TestMethod]
        public void ValidateDeleteItemRequestThrowsWhenKeyIsEmpty()
        {
            var request = new DeleteItemRequest
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>()
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("key"));
        }

        [TestMethod]
        public void ValidateTransactGetItemsRequestThrowsWhenTableNameIsNull()
        {
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = null,
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { "Id", new AttributeValue { S = "test" } }
                            }
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("TableName"));
        }

        [TestMethod]
        public void ValidateTransactGetItemsRequestThrowsWhenKeyIsEmpty()
        {
            var request = new TransactGetItemsRequest
            {
                TransactItems = new List<TransactGetItem>
                {
                    new TransactGetItem
                    {
                        Get = new Get
                        {
                            TableName = "TestTable",
                            Key = new Dictionary<string, AttributeValue>()
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("key"));
        }

        [TestMethod]
        public void ValidateTransactWriteItemsRequestThrowsWhenTableNameIsNull()
        {
            var request = new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = null,
                            Item = new Dictionary<string, AttributeValue>()
                        }
                    }
                }
            };

            var exception = Assert.ThrowsException<InvalidOperationException>(() =>
            {
                AmazonDynamoDBWithTransactionsTestHelper.ValidateRequest(request);
            });

            Assert.IsTrue(exception.Message.Contains("TableName"));
        }

        [TestMethod]
        public void CombineJoinsExpressionsWithAnd()
        {
            var result = AmazonDynamoDBWithTransactionsTestHelper.Combine("expr1", "expr2", "expr3");
            Assert.AreEqual("expr1 AND expr2 AND expr3", result);
        }

        [TestMethod]
        public void CombineIgnoresNullAndWhitespace()
        {
            var result = AmazonDynamoDBWithTransactionsTestHelper.Combine("expr1", null, "", "  ", "expr2");
            Assert.AreEqual("expr1 AND expr2", result);
        }

        [TestMethod]
        public void CombineReturnsEmptyWhenAllNullOrWhitespace()
        {
            var result = AmazonDynamoDBWithTransactionsTestHelper.Combine(null, "", "  ");
            Assert.AreEqual("", result);
        }

        [TestMethod]
        public void IsSupportedConditionExpressionReturnsTrueForAttributeNotExists()
        {
            var conditionCheck = new ConditionCheck
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                },
                ConditionExpression = "attribute_not_exists(Id)"
            };

            var result = AmazonDynamoDBWithTransactionsTestHelper.IsSupportedConditionExpression(conditionCheck, "attribute_not_exists");
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsSupportedConditionExpressionReturnsFalseForDifferentExpression()
        {
            var conditionCheck = new ConditionCheck
            {
                TableName = "TestTable",
                Key = new Dictionary<string, AttributeValue>
                {
                    { "Id", new AttributeValue { S = "test" } }
                },
                ConditionExpression = "attribute_exists(Id)"
            };

            var result = AmazonDynamoDBWithTransactionsTestHelper.IsSupportedConditionExpression(conditionCheck, "attribute_not_exists");
            Assert.IsFalse(result);
        }
    }

    public static class AmazonDynamoDBWithTransactionsTestHelper
    {
        public static void ValidateRequest(GetItemRequest request)
        {
            if (request.AttributesToGet?.Count > 0)
            {
                throw new NotSupportedException("Legacy attributes on requests are not supported");
            }

            if (string.IsNullOrWhiteSpace(request.TableName))
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.Key?.Count == 0)
            {
                throw new InvalidOperationException("The request key cannot be empty");
            }
        }

        public static void ValidateRequest(PutItemRequest request)
        {
            if (request.ConditionalOperator != null || (request.Expected?.Count > 0))
            {
                throw new NotSupportedException("Legacy attributes on requests are not supported");
            }

            if (string.IsNullOrWhiteSpace(request.TableName))
            {
                throw new InvalidOperationException("TableName must not be null");
            }
        }

        public static void ValidateRequest(UpdateItemRequest request)
        {
            if (request.ConditionalOperator != null || (request.Expected?.Count > 0) || (request.AttributeUpdates?.Count > 0))
            {
                throw new NotSupportedException("Legacy attributes on requests are not supported");
            }

            if (string.IsNullOrWhiteSpace(request.TableName))
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.Key?.Count == 0)
            {
                throw new InvalidOperationException("The request key cannot be empty");
            }
        }

        public static void ValidateRequest(DeleteItemRequest request)
        {
            if (request.ConditionalOperator != null || (request.Expected?.Count > 0))
            {
                throw new NotSupportedException("Legacy attributes on requests are not supported");
            }

            if (string.IsNullOrWhiteSpace(request.TableName))
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.Key?.Count == 0)
            {
                throw new InvalidOperationException("The request key cannot be empty");
            }
        }

        public static void ValidateRequest(TransactGetItemsRequest request)
        {
            if (request.TransactItems != null && request.TransactItems.Any(v => string.IsNullOrWhiteSpace(v.Get.TableName)))
            {
                throw new InvalidOperationException("TableName must not be null");
            }

            if (request.TransactItems != null && request.TransactItems.Any(v => v.Get.Key?.Count == 0))
            {
                throw new InvalidOperationException("The request key cannot be empty");
            }
        }

        public static void ValidateRequest(TransactWriteItemsRequest request)
        {
            if (request.TransactItems != null && request.TransactItems.Any(v => string.IsNullOrWhiteSpace(v.ConditionCheck?.TableName ?? v.Delete?.TableName ?? v.Put?.TableName ?? v.Update?.TableName)))
            {
                throw new InvalidOperationException("TableName must not be null");
            }
        }

        public static string Combine(params string?[] expressions)
        {
            return string.Join(" AND ", expressions.Where(e => !string.IsNullOrWhiteSpace(e)).Select(e => e?.Trim()));
        }

        public static bool IsSupportedConditionExpression(ConditionCheck conditionCheck, string conditionExpressionFunction)
        {
            return conditionCheck.Key.Keys.Any(key => conditionCheck.ConditionExpression == $"{conditionExpressionFunction}({key})");
        }
    }
}
