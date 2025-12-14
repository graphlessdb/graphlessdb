/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Generic;
using System.Globalization;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services.DynamoDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.DynamoDB.Storage.Services.Tests
{
    [TestClass]
    public sealed class AmazonDynamoDBRDFTripleItemServiceTests
    {
        private static AmazonDynamoDBRDFTripleItemService CreateService()
        {
            return new AmazonDynamoDBRDFTripleItemService();
        }

        [TestMethod]
        public void ToAttributeMapWithRDFTripleKeyReturnsCorrectAttributeMap()
        {
            var service = CreateService();
            var key = new RDFTripleKey("test-subject", "test-predicate");

            var result = service.ToAttributeMap(key);

            Assert.AreEqual(2, result.Count);
            Assert.IsTrue(result.ContainsKey("Subject"));
            Assert.AreEqual("test-subject", result["Subject"].S);
            Assert.IsTrue(result.ContainsKey("Predicate"));
            Assert.AreEqual("test-predicate", result["Predicate"].S);
        }

        [TestMethod]
        public void ToAttributeMapWithRDFTripleKeyWithPartitionReturnsCorrectAttributeMap()
        {
            var service = CreateService();
            var key = new RDFTripleKeyWithPartition("test-subject", "test-predicate", "test-partition");

            var result = service.ToAttributeMap(key);

            Assert.AreEqual(3, result.Count);
            Assert.IsTrue(result.ContainsKey("Subject"));
            Assert.AreEqual("test-subject", result["Subject"].S);
            Assert.IsTrue(result.ContainsKey("Predicate"));
            Assert.AreEqual("test-predicate", result["Predicate"].S);
            Assert.IsTrue(result.ContainsKey("Partition"));
            Assert.AreEqual("test-partition", result["Partition"].S);
        }

        [TestMethod]
        public void ToAttributeMapWithRDFTripleWithoutVersionDetailReturnsCorrectAttributeMap()
        {
            var service = CreateService();
            var triple = new RDFTriple(
                "test-subject",
                "test-predicate",
                "test-indexed-object",
                "test-object",
                "test-partition",
                null);

            var result = service.ToAttributeMap(triple);

            Assert.AreEqual(5, result.Count);
            Assert.IsTrue(result.ContainsKey("Subject"));
            Assert.AreEqual("test-subject", result["Subject"].S);
            Assert.IsTrue(result.ContainsKey("Predicate"));
            Assert.AreEqual("test-predicate", result["Predicate"].S);
            Assert.IsTrue(result.ContainsKey("IndexedObject"));
            Assert.AreEqual("test-indexed-object", result["IndexedObject"].S);
            Assert.IsTrue(result.ContainsKey("Object"));
            Assert.AreEqual("test-object", result["Object"].S);
            Assert.IsTrue(result.ContainsKey("Partition"));
            Assert.AreEqual("test-partition", result["Partition"].S);
            Assert.IsFalse(result.ContainsKey("VersionDetail"));
        }

        [TestMethod]
        public void ToAttributeMapWithRDFTripleWithVersionDetailReturnsCorrectAttributeMap()
        {
            var service = CreateService();
            var versionDetail = new VersionDetail(5, 10);
            var triple = new RDFTriple(
                "test-subject",
                "test-predicate",
                "test-indexed-object",
                "test-object",
                "test-partition",
                versionDetail);

            var result = service.ToAttributeMap(triple);

            Assert.AreEqual(6, result.Count);
            Assert.IsTrue(result.ContainsKey("Subject"));
            Assert.AreEqual("test-subject", result["Subject"].S);
            Assert.IsTrue(result.ContainsKey("Predicate"));
            Assert.AreEqual("test-predicate", result["Predicate"].S);
            Assert.IsTrue(result.ContainsKey("IndexedObject"));
            Assert.AreEqual("test-indexed-object", result["IndexedObject"].S);
            Assert.IsTrue(result.ContainsKey("Object"));
            Assert.AreEqual("test-object", result["Object"].S);
            Assert.IsTrue(result.ContainsKey("Partition"));
            Assert.AreEqual("test-partition", result["Partition"].S);
            Assert.IsTrue(result.ContainsKey("VersionDetail"));
            Assert.IsNotNull(result["VersionDetail"].M);
            Assert.AreEqual(2, result["VersionDetail"].M.Count);
            Assert.AreEqual("5", result["VersionDetail"].M["NodeVersion"].N);
            Assert.AreEqual("10", result["VersionDetail"].M["AllEdgesVersion"].N);
        }

        [TestMethod]
        public void IsRDFTripleReturnsTrueForCompleteRDFTriple()
        {
            var service = CreateService();
            var attributeMap = new Dictionary<string, AttributeValue>
            {
                { "Subject", AttributeValueFactory.CreateS("test-subject") },
                { "Predicate", AttributeValueFactory.CreateS("test-predicate") },
                { "Object", AttributeValueFactory.CreateS("test-object") },
                { "Partition", AttributeValueFactory.CreateS("test-partition") }
            };

            var result = service.IsRDFTriple(attributeMap);

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsRDFTripleReturnsFalseWhenMissingSubject()
        {
            var service = CreateService();
            var attributeMap = new Dictionary<string, AttributeValue>
            {
                { "Predicate", AttributeValueFactory.CreateS("test-predicate") },
                { "Object", AttributeValueFactory.CreateS("test-object") },
                { "Partition", AttributeValueFactory.CreateS("test-partition") }
            };

            var result = service.IsRDFTriple(attributeMap);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsRDFTripleReturnsFalseWhenMissingPredicate()
        {
            var service = CreateService();
            var attributeMap = new Dictionary<string, AttributeValue>
            {
                { "Subject", AttributeValueFactory.CreateS("test-subject") },
                { "Object", AttributeValueFactory.CreateS("test-object") },
                { "Partition", AttributeValueFactory.CreateS("test-partition") }
            };

            var result = service.IsRDFTriple(attributeMap);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsRDFTripleReturnsFalseWhenMissingObject()
        {
            var service = CreateService();
            var attributeMap = new Dictionary<string, AttributeValue>
            {
                { "Subject", AttributeValueFactory.CreateS("test-subject") },
                { "Predicate", AttributeValueFactory.CreateS("test-predicate") },
                { "Partition", AttributeValueFactory.CreateS("test-partition") }
            };

            var result = service.IsRDFTriple(attributeMap);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsRDFTripleReturnsFalseWhenMissingPartition()
        {
            var service = CreateService();
            var attributeMap = new Dictionary<string, AttributeValue>
            {
                { "Subject", AttributeValueFactory.CreateS("test-subject") },
                { "Predicate", AttributeValueFactory.CreateS("test-predicate") },
                { "Object", AttributeValueFactory.CreateS("test-object") }
            };

            var result = service.IsRDFTriple(attributeMap);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsRDFTripleReturnsFalseForEmptyDictionary()
        {
            var service = CreateService();
            var attributeMap = new Dictionary<string, AttributeValue>();

            var result = service.IsRDFTriple(attributeMap);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void ToRDFTripleConvertsAttributeMapToRDFTripleWithoutVersionDetail()
        {
            var service = CreateService();
            var attributeMap = new Dictionary<string, AttributeValue>
            {
                { "Subject", AttributeValueFactory.CreateS("test-subject") },
                { "Predicate", AttributeValueFactory.CreateS("test-predicate") },
                { "IndexedObject", AttributeValueFactory.CreateS("test-indexed-object") },
                { "Object", AttributeValueFactory.CreateS("test-object") },
                { "Partition", AttributeValueFactory.CreateS("test-partition") }
            };

            var result = service.ToRDFTriple(attributeMap);

            Assert.AreEqual("test-subject", result.Subject);
            Assert.AreEqual("test-predicate", result.Predicate);
            Assert.AreEqual("test-indexed-object", result.IndexedObject);
            Assert.AreEqual("test-object", result.Object);
            Assert.AreEqual("test-partition", result.Partition);
            Assert.IsNull(result.VersionDetail);
        }

        [TestMethod]
        public void ToRDFTripleConvertsAttributeMapToRDFTripleWithVersionDetail()
        {
            var service = CreateService();
            var versionDetailMap = new Dictionary<string, AttributeValue>
            {
                { "NodeVersion", AttributeValueFactory.CreateN("7") },
                { "AllEdgesVersion", AttributeValueFactory.CreateN("14") }
            };
            var attributeMap = new Dictionary<string, AttributeValue>
            {
                { "Subject", AttributeValueFactory.CreateS("test-subject") },
                { "Predicate", AttributeValueFactory.CreateS("test-predicate") },
                { "IndexedObject", AttributeValueFactory.CreateS("test-indexed-object") },
                { "Object", AttributeValueFactory.CreateS("test-object") },
                { "Partition", AttributeValueFactory.CreateS("test-partition") },
                { "VersionDetail", AttributeValueFactory.CreateM(versionDetailMap) }
            };

            var result = service.ToRDFTriple(attributeMap);

            Assert.AreEqual("test-subject", result.Subject);
            Assert.AreEqual("test-predicate", result.Predicate);
            Assert.AreEqual("test-indexed-object", result.IndexedObject);
            Assert.AreEqual("test-object", result.Object);
            Assert.AreEqual("test-partition", result.Partition);
            Assert.IsNotNull(result.VersionDetail);
            Assert.AreEqual(7, result.VersionDetail.NodeVersion);
            Assert.AreEqual(14, result.VersionDetail.AllEdgesVersion);
        }

        [TestMethod]
        public void ToRDFTripleUsesSpaceForMissingIndexedObject()
        {
            var service = CreateService();
            var attributeMap = new Dictionary<string, AttributeValue>
            {
                { "Subject", AttributeValueFactory.CreateS("test-subject") },
                { "Predicate", AttributeValueFactory.CreateS("test-predicate") },
                { "Object", AttributeValueFactory.CreateS("test-object") },
                { "Partition", AttributeValueFactory.CreateS("test-partition") }
            };

            var result = service.ToRDFTriple(attributeMap);

            Assert.AreEqual("test-subject", result.Subject);
            Assert.AreEqual("test-predicate", result.Predicate);
            Assert.AreEqual(" ", result.IndexedObject);
            Assert.AreEqual("test-object", result.Object);
            Assert.AreEqual("test-partition", result.Partition);
            Assert.IsNull(result.VersionDetail);
        }

        [TestMethod]
        public void ToAttributeMapAndToRDFTripleRoundTripWithoutVersionDetail()
        {
            var service = CreateService();
            var originalTriple = new RDFTriple(
                "test-subject",
                "test-predicate",
                "test-indexed-object",
                "test-object",
                "test-partition",
                null);

            var attributeMap = service.ToAttributeMap(originalTriple);
            var resultTriple = service.ToRDFTriple(attributeMap);

            Assert.AreEqual(originalTriple.Subject, resultTriple.Subject);
            Assert.AreEqual(originalTriple.Predicate, resultTriple.Predicate);
            Assert.AreEqual(originalTriple.IndexedObject, resultTriple.IndexedObject);
            Assert.AreEqual(originalTriple.Object, resultTriple.Object);
            Assert.AreEqual(originalTriple.Partition, resultTriple.Partition);
            Assert.AreEqual(originalTriple.VersionDetail, resultTriple.VersionDetail);
        }

        [TestMethod]
        public void ToAttributeMapAndToRDFTripleRoundTripWithVersionDetail()
        {
            var service = CreateService();
            var versionDetail = new VersionDetail(3, 6);
            var originalTriple = new RDFTriple(
                "test-subject",
                "test-predicate",
                "test-indexed-object",
                "test-object",
                "test-partition",
                versionDetail);

            var attributeMap = service.ToAttributeMap(originalTriple);
            var resultTriple = service.ToRDFTriple(attributeMap);

            Assert.AreEqual(originalTriple.Subject, resultTriple.Subject);
            Assert.AreEqual(originalTriple.Predicate, resultTriple.Predicate);
            Assert.AreEqual(originalTriple.IndexedObject, resultTriple.IndexedObject);
            Assert.AreEqual(originalTriple.Object, resultTriple.Object);
            Assert.AreEqual(originalTriple.Partition, resultTriple.Partition);
            Assert.IsNotNull(resultTriple.VersionDetail);
            Assert.AreEqual(originalTriple.VersionDetail!.NodeVersion, resultTriple.VersionDetail.NodeVersion);
            Assert.AreEqual(originalTriple.VersionDetail!.AllEdgesVersion, resultTriple.VersionDetail.AllEdgesVersion);
        }


        [TestMethod]
        public void ToAttributeMapWithRDFTripleHandlesVersionDetailWithZeroValues()
        {
            var service = CreateService();
            var versionDetail = new VersionDetail(0, 0);
            var triple = new RDFTriple(
                "test-subject",
                "test-predicate",
                "test-indexed-object",
                "test-object",
                "test-partition",
                versionDetail);

            var result = service.ToAttributeMap(triple);

            Assert.AreEqual(6, result.Count);
            Assert.IsTrue(result.ContainsKey("VersionDetail"));
            Assert.AreEqual("0", result["VersionDetail"].M["NodeVersion"].N);
            Assert.AreEqual("0", result["VersionDetail"].M["AllEdgesVersion"].N);
        }

        [TestMethod]
        public void ToRDFTripleHandlesVersionDetailWithZeroValues()
        {
            var service = CreateService();
            var versionDetailMap = new Dictionary<string, AttributeValue>
            {
                { "NodeVersion", AttributeValueFactory.CreateN("0") },
                { "AllEdgesVersion", AttributeValueFactory.CreateN("0") }
            };
            var attributeMap = new Dictionary<string, AttributeValue>
            {
                { "Subject", AttributeValueFactory.CreateS("test-subject") },
                { "Predicate", AttributeValueFactory.CreateS("test-predicate") },
                { "IndexedObject", AttributeValueFactory.CreateS("test-indexed-object") },
                { "Object", AttributeValueFactory.CreateS("test-object") },
                { "Partition", AttributeValueFactory.CreateS("test-partition") },
                { "VersionDetail", AttributeValueFactory.CreateM(versionDetailMap) }
            };

            var result = service.ToRDFTriple(attributeMap);

            Assert.IsNotNull(result.VersionDetail);
            Assert.AreEqual(0, result.VersionDetail.NodeVersion);
            Assert.AreEqual(0, result.VersionDetail.AllEdgesVersion);
        }

        [TestMethod]
        public void ToRDFTripleHandlesVersionDetailWithLargeValues()
        {
            var service = CreateService();
            var versionDetailMap = new Dictionary<string, AttributeValue>
            {
                { "NodeVersion", AttributeValueFactory.CreateN(int.MaxValue.ToString(CultureInfo.InvariantCulture)) },
                { "AllEdgesVersion", AttributeValueFactory.CreateN(int.MaxValue.ToString(CultureInfo.InvariantCulture)) }
            };
            var attributeMap = new Dictionary<string, AttributeValue>
            {
                { "Subject", AttributeValueFactory.CreateS("test-subject") },
                { "Predicate", AttributeValueFactory.CreateS("test-predicate") },
                { "IndexedObject", AttributeValueFactory.CreateS("test-indexed-object") },
                { "Object", AttributeValueFactory.CreateS("test-object") },
                { "Partition", AttributeValueFactory.CreateS("test-partition") },
                { "VersionDetail", AttributeValueFactory.CreateM(versionDetailMap) }
            };

            var result = service.ToRDFTriple(attributeMap);

            Assert.IsNotNull(result.VersionDetail);
            Assert.AreEqual(int.MaxValue, result.VersionDetail.NodeVersion);
            Assert.AreEqual(int.MaxValue, result.VersionDetail.AllEdgesVersion);
        }

        [TestMethod]
        public void ToAttributeMapWithRDFTripleHandlesSpecialCharacters()
        {
            var service = CreateService();
            var triple = new RDFTriple(
                "subject/with\\special@chars",
                "predicate:with#special$chars",
                "indexed!object",
                "object~with*special&chars",
                "partition|with%special^chars",
                null);

            var result = service.ToAttributeMap(triple);

            Assert.AreEqual("subject/with\\special@chars", result["Subject"].S);
            Assert.AreEqual("predicate:with#special$chars", result["Predicate"].S);
            Assert.AreEqual("indexed!object", result["IndexedObject"].S);
            Assert.AreEqual("object~with*special&chars", result["Object"].S);
            Assert.AreEqual("partition|with%special^chars", result["Partition"].S);
        }

        [TestMethod]
        public void ToRDFTripleHandlesSpecialCharacters()
        {
            var service = CreateService();
            var attributeMap = new Dictionary<string, AttributeValue>
            {
                { "Subject", AttributeValueFactory.CreateS("subject/with\\special@chars") },
                { "Predicate", AttributeValueFactory.CreateS("predicate:with#special$chars") },
                { "IndexedObject", AttributeValueFactory.CreateS("indexed!object") },
                { "Object", AttributeValueFactory.CreateS("object~with*special&chars") },
                { "Partition", AttributeValueFactory.CreateS("partition|with%special^chars") }
            };

            var result = service.ToRDFTriple(attributeMap);

            Assert.AreEqual("subject/with\\special@chars", result.Subject);
            Assert.AreEqual("predicate:with#special$chars", result.Predicate);
            Assert.AreEqual("indexed!object", result.IndexedObject);
            Assert.AreEqual("object~with*special&chars", result.Object);
            Assert.AreEqual("partition|with%special^chars", result.Partition);
        }
    }
}
