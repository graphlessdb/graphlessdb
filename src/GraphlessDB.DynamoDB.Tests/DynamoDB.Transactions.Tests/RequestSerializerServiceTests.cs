/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Generic;
using System.IO;
using System.Text;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.DynamoDB.Transactions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Transactions.Tests
{
    [TestClass]
    public sealed class RequestSerializerServiceTests
    {
        [TestMethod]
        public void CanSerializeAndDeserializeGetItemRequest()
        {
            var service = new RequestRecordSerializer();
            var request = new RequestRecord(
                0,
                new GetItemRequest
                {
                    TableName = "Test",
                    Key = new Dictionary<string, AttributeValue>
                    {{
                        "Id", AttributeValueFactory.CreateS("IdValue")
                    }}
                }, null, null, null, null, null);

            var bytes = service.Serialize(request);
            var deserializedRequest = service.Deserialize(bytes);
            Assert.IsTrue(TestHelpers.AreEqual(request, deserializedRequest));
        }

        [TestMethod]
        public void CanSerialiseB()
        {
            var service = new RequestRecordSerializer();
            var request = new RequestRecord(
                0,
                new GetItemRequest
                {
                    TableName = "Test",
                    Key = new Dictionary<string, AttributeValue>
                    {{
                        "Binary", AttributeValueFactory.CreateB(new MemoryStream(Encoding.UTF8.GetBytes("asdf\n\t\u0123")))
                    }}
                }, null, null, null, null, null);

            var bytes = service.Serialize(request);
            var deserializedRequest = service.Deserialize(bytes);
            Assert.IsTrue(TestHelpers.AreEqual(request, deserializedRequest));
        }

        [TestMethod]
        public void CanSerialiseBS()
        {
            var service = new RequestRecordSerializer();
            var request = new RequestRecord(
                0,
                new GetItemRequest
                {
                    TableName = "Test",
                    Key = new Dictionary<string, AttributeValue>
                    {{
                        "Binary",
                        AttributeValueFactory.CreateBS(
                        [
                            new(Encoding.UTF8.GetBytes("asdf\n\t\u0123")),
                            new(Encoding.UTF8.GetBytes("wef"))
                        ])
                    }}
                }, null, null, null, null, null);

            var bytes = service.Serialize(request);
            var deserializedRequest = service.Deserialize(bytes);
            Assert.IsTrue(TestHelpers.AreEqual(request, deserializedRequest));
        }
    }
}