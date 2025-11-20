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
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;
using GraphlessDB.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Graph.Services.Internal.Tests
{
    [TestClass]
    public sealed class RDFTripleFactoryTests
    {
        private const string GraphName = "TestGraph";
        private const string TableName = "TestTable";
        private const string Partition = "0";

        private sealed class MockGraphSettingsService : IGraphSettingsService
        {
            private readonly GraphSettings _settings;

            public MockGraphSettingsService(GraphSettings settings)
            {
                _settings = settings;
            }

            public GraphSettings GetGraphSettings() => _settings;
        }

        private sealed class MockGraphPartitionService : IGraphPartitionService
        {
            public string GetPartition(string id) => Partition;
        }

        private sealed class MockGraphEntityTypeService : IGraphEntityTypeService
        {
            private readonly Dictionary<string, Type> _types = new Dictionary<string, Type>();

            public void RegisterType<T>(string typeName)
            {
                _types[typeName] = typeof(T);
            }

            public Type GetEntityType(string typeName)
            {
                if (_types.TryGetValue(typeName, out var type))
                {
                    return type;
                }

                throw new InvalidOperationException($"Type not found: {typeName}");
            }
        }

        private sealed class MockGraphQueryablePropertyService : IGraphQueryablePropertyService
        {
            private readonly HashSet<string> _queryableProperties = new HashSet<string>();

            public void RegisterQueryableProperty(string typeName, string propertyName)
            {
                _queryableProperties.Add($"{typeName}.{propertyName}");
            }

            public bool IsQueryableProperty(string typeName, string propertyName)
            {
                return _queryableProperties.Contains($"{typeName}.{propertyName}");
            }
        }

        private sealed class MockGraphEntitySerializationService : IGraphEntitySerializationService
        {
            public string SerializeNode(INode node, Type type)
            {
                return $"{{\"Id\":\"{node.Id}\"}}";
            }

            public string SerializeEdge(IEdge edge, Type type)
            {
                return $"{{\"InId\":\"{edge.InId}\",\"OutId\":\"{edge.OutId}\"}}";
            }

            public INode DeserializeNode(string value, Type type)
            {
                if (type == typeof(TestNode))
                {
                    var id = value.Split('"')[3];
                    return new TestNode(id);
                }

                throw new NotImplementedException();
            }

            public IEdge DeserializeEdge(string value, Type type)
            {
                if (type == typeof(TestEdge))
                {
                    var parts = value.Split('"');
                    var inId = parts[3];
                    var outId = parts[7];
                    return new TestEdge(inId, outId);
                }

                throw new NotImplementedException();
            }
        }

        private sealed class MockGraphSerializationService : IGraphSerializationService
        {
            public string GetPropertyAsString(object? value)
            {
                return value?.ToString() ?? string.Empty;
            }
        }

        private sealed record TestNode(string Id) : INode(
            Id,
            new VersionDetail(1, 0),
            DateTime.UtcNow,
            DateTime.UtcNow,
            DateTime.MinValue);

        private sealed record TestNodeWithProperty(string Id, string Name) : INode(
            Id,
            new VersionDetail(1, 0),
            DateTime.UtcNow,
            DateTime.UtcNow,
            DateTime.MinValue);

        private sealed record TestEdge(string InId, string OutId) : IEdge(
            DateTime.UtcNow,
            DateTime.UtcNow,
            DateTime.MinValue,
            InId,
            OutId);

        private sealed record TestEdgeWithProperty(string InId, string OutId, string Label) : IEdge(
            DateTime.UtcNow,
            DateTime.UtcNow,
            DateTime.MinValue,
            InId,
            OutId);

        private sealed record TestDeletedEdge(string InId, string OutId, DateTime CreatedAt, DateTime DeletedAt) : IEdge(
            CreatedAt,
            DateTime.UtcNow,
            DeletedAt,
            InId,
            OutId);

        private static RDFTripleFactory CreateFactory(
            MockGraphSettingsService? graphSettingsService = null,
            MockGraphPartitionService? partitionService = null,
            MockGraphEntityTypeService? typeService = null,
            MockGraphQueryablePropertyService? queryablePropertyService = null,
            MockGraphEntitySerializationService? entitySerializer = null,
            MockGraphSerializationService? entityValueSerializer = null)
        {
            graphSettingsService ??= new MockGraphSettingsService(
                new GraphSettings(TableName, GraphName, 1, "byPred", "byObj"));
            partitionService ??= new MockGraphPartitionService();
            typeService ??= new MockGraphEntityTypeService();
            queryablePropertyService ??= new MockGraphQueryablePropertyService();
            entitySerializer ??= new MockGraphEntitySerializationService();
            entityValueSerializer ??= new MockGraphSerializationService();

            return new RDFTripleFactory(
                graphSettingsService,
                partitionService,
                typeService,
                queryablePropertyService,
                entitySerializer,
                entityValueSerializer);
        }

        [TestMethod]
        public void HasTypeReturnsCorrectRDFTriple()
        {
            var factory = CreateFactory();
            var node = new TestNode(GlobalId.Get<TestNode>("test-id"));

            var result = factory.HasType(node);

            Assert.AreEqual(node.Id, result.Subject);
            Assert.IsTrue(result.Predicate.StartsWith($"{GraphName}#type#TestNode#", StringComparison.Ordinal));
            Assert.AreEqual(" ", result.IndexedObject);
            Assert.AreEqual($"{{\"Id\":\"{node.Id}\"}}", result.Object);
            Assert.AreEqual(Partition, result.Partition);
            Assert.AreEqual(node.Version, result.VersionDetail);
        }

        [TestMethod]
        public void HasBlobReturnsCorrectRDFTriple()
        {
            var factory = CreateFactory();
            var node = new TestNode(GlobalId.Get<TestNode>("test-id"));

            var result = factory.HasBlob(node);

            Assert.AreEqual(node.Id, result.Subject);
            Assert.AreEqual($"{GraphName}#blob#TestNode#{node.Version.NodeVersion}", result.Predicate);
            Assert.AreEqual(" ", result.IndexedObject);
            Assert.AreEqual($"{{\"Id\":\"{node.Id}\"}}", result.Object);
            Assert.AreEqual(Partition, result.Partition);
            Assert.IsNull(result.VersionDetail);
        }

        [TestMethod]
        public void HasPropReturnsCorrectRDFTriple()
        {
            var factory = CreateFactory();
            var node = new TestNode(GlobalId.Get<TestNode>("test-id"));
            var propertyName = "TestProperty";
            var propertyValue = "TestValue";

            var result = factory.HasProp(node, propertyName, propertyValue);

            Assert.AreEqual(node.Id, result.Subject);
            Assert.IsTrue(result.Predicate.Contains(propertyName, StringComparison.Ordinal));
            Assert.AreEqual(propertyValue, result.IndexedObject);
            Assert.AreEqual(propertyValue, result.Object);
            Assert.AreEqual(Partition, result.Partition);
            Assert.IsNull(result.VersionDetail);
        }

        [TestMethod]
        public void HasPropHandlesEmptyStringPropertyValue()
        {
            var factory = CreateFactory();
            var node = new TestNode(GlobalId.Get<TestNode>("test-id"));
            var propertyName = "TestProperty";
            var propertyValue = string.Empty;

            var result = factory.HasProp(node, propertyName, propertyValue);

            Assert.AreEqual(" ", result.IndexedObject);
            Assert.AreEqual(string.Empty, result.Object);
        }

        [TestMethod]
        public void HasPropTruncatesLongPropertyValue()
        {
            var factory = CreateFactory();
            var node = new TestNode(GlobalId.Get<TestNode>("test-id"));
            var propertyName = "TestProperty";
            var propertyValue = new string('a', 150);

            var result = factory.HasProp(node, propertyName, propertyValue);

            Assert.AreEqual(100, result.IndexedObject.Length);
            Assert.AreEqual(150, result.Object.Length);
        }

        [TestMethod]
        public void HasInEdgeReturnsCorrectRDFTriple()
        {
            var factory = CreateFactory();
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdge(inId, outId);

            var result = factory.HasInEdge(edge);

            Assert.AreEqual(inId, result.Subject);
            Assert.IsTrue(result.Predicate.Contains("TestNode", StringComparison.Ordinal));
            Assert.IsTrue(result.Predicate.Contains("TestEdge", StringComparison.Ordinal));
            Assert.AreEqual(" ", result.IndexedObject);
            Assert.AreEqual($"{{\"InId\":\"{inId}\",\"OutId\":\"{outId}\"}}", result.Object);
            Assert.AreEqual(Partition, result.Partition);
            Assert.IsNull(result.VersionDetail);
        }

        [TestMethod]
        public void HasInEdgePropReturnsCorrectRDFTriple()
        {
            var factory = CreateFactory();
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdge(inId, outId);
            var propertyName = "TestProperty";
            var propertyValue = "TestValue";

            var result = factory.HasInEdgeProp(edge, propertyName, propertyValue);

            Assert.AreEqual(inId, result.Subject);
            Assert.IsTrue(result.Predicate.Contains(propertyName, StringComparison.Ordinal));
            Assert.AreEqual(propertyValue, result.IndexedObject);
            Assert.AreEqual(propertyValue, result.Object);
            Assert.AreEqual(Partition, result.Partition);
            Assert.IsNull(result.VersionDetail);
        }

        [TestMethod]
        public void HasInEdgePropHandlesEmptyStringPropertyValue()
        {
            var factory = CreateFactory();
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdge(inId, outId);
            var propertyName = "TestProperty";
            var propertyValue = string.Empty;

            var result = factory.HasInEdgeProp(edge, propertyName, propertyValue);

            Assert.AreEqual(" ", result.IndexedObject);
            Assert.AreEqual(string.Empty, result.Object);
        }

        [TestMethod]
        public void HasInEdgePropTruncatesLongPropertyValue()
        {
            var factory = CreateFactory();
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdge(inId, outId);
            var propertyName = "TestProperty";
            var propertyValue = new string('a', 150);

            var result = factory.HasInEdgeProp(edge, propertyName, propertyValue);

            Assert.AreEqual(100, result.IndexedObject.Length);
            Assert.AreEqual(150, result.Object.Length);
        }

        [TestMethod]
        public void HadInEdgeReturnsCorrectRDFTriple()
        {
            var factory = CreateFactory();
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 12, 31, 23, 59, 59, DateTimeKind.Utc);
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestDeletedEdge(inId, outId, createdAt, deletedAt);

            var result = factory.HadInEdge(edge);

            Assert.AreEqual(inId, result.Subject);
            Assert.IsTrue(result.Predicate.Contains("TestNode", StringComparison.Ordinal));
            Assert.IsTrue(result.Predicate.Contains("TestDeletedEdge", StringComparison.Ordinal));
            Assert.AreEqual(" ", result.IndexedObject);
            Assert.AreEqual($"{{\"InId\":\"{inId}\",\"OutId\":\"{outId}\"}}", result.Object);
            Assert.AreEqual(Partition, result.Partition);
            Assert.IsNull(result.VersionDetail);
        }

        [TestMethod]
        public void HasOutEdgeReturnsCorrectRDFTriple()
        {
            var factory = CreateFactory();
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdge(inId, outId);

            var result = factory.HasOutEdge(edge);

            Assert.AreEqual(outId, result.Subject);
            Assert.IsTrue(result.Predicate.Contains("TestNode", StringComparison.Ordinal));
            Assert.IsTrue(result.Predicate.Contains("TestEdge", StringComparison.Ordinal));
            Assert.AreEqual(" ", result.IndexedObject);
            Assert.AreEqual($"{{\"InId\":\"{inId}\",\"OutId\":\"{outId}\"}}", result.Object);
            Assert.AreEqual(Partition, result.Partition);
            Assert.IsNull(result.VersionDetail);
        }

        [TestMethod]
        public void HasOutEdgePropReturnsCorrectRDFTriple()
        {
            var factory = CreateFactory();
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdge(inId, outId);
            var propertyName = "TestProperty";
            var propertyValue = "TestValue";

            var result = factory.HasOutEdgeProp(edge, propertyName, propertyValue);

            Assert.AreEqual(outId, result.Subject);
            Assert.IsTrue(result.Predicate.Contains(propertyName, StringComparison.Ordinal));
            Assert.AreEqual(propertyValue, result.IndexedObject);
            Assert.AreEqual(propertyValue, result.Object);
            Assert.AreEqual(Partition, result.Partition);
            Assert.IsNull(result.VersionDetail);
        }

        [TestMethod]
        public void HasOutEdgePropHandlesEmptyStringPropertyValue()
        {
            var factory = CreateFactory();
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdge(inId, outId);
            var propertyName = "TestProperty";
            var propertyValue = string.Empty;

            var result = factory.HasOutEdgeProp(edge, propertyName, propertyValue);

            Assert.AreEqual(" ", result.IndexedObject);
            Assert.AreEqual(string.Empty, result.Object);
        }

        [TestMethod]
        public void HasOutEdgePropTruncatesLongPropertyValue()
        {
            var factory = CreateFactory();
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdge(inId, outId);
            var propertyName = "TestProperty";
            var propertyValue = new string('a', 150);

            var result = factory.HasOutEdgeProp(edge, propertyName, propertyValue);

            Assert.AreEqual(100, result.IndexedObject.Length);
            Assert.AreEqual(150, result.Object.Length);
        }

        [TestMethod]
        public void HadOutEdgeReturnsCorrectRDFTriple()
        {
            var factory = CreateFactory();
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 12, 31, 23, 59, 59, DateTimeKind.Utc);
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestDeletedEdge(inId, outId, createdAt, deletedAt);

            var result = factory.HadOutEdge(edge);

            Assert.AreEqual(outId, result.Subject);
            Assert.IsTrue(result.Predicate.Contains("TestNode", StringComparison.Ordinal));
            Assert.IsTrue(result.Predicate.Contains("TestDeletedEdge", StringComparison.Ordinal));
            Assert.AreEqual(" ", result.IndexedObject);
            Assert.AreEqual($"{{\"InId\":\"{inId}\",\"OutId\":\"{outId}\"}}", result.Object);
            Assert.AreEqual(Partition, result.Partition);
            Assert.IsNull(result.VersionDetail);
        }

        [TestMethod]
        public void GetNodeReturnsDeserializedNode()
        {
            var typeService = new MockGraphEntityTypeService();
            typeService.RegisterType<TestNode>("TestNode");
            var factory = CreateFactory(typeService: typeService);
            var nodeId = GlobalId.Get<TestNode>("test-id");
            var versionDetail = new VersionDetail(1, 0);
            var triple = new RDFTriple(
                nodeId,
                $"{GraphName}#type#TestNode#{nodeId}",
                " ",
                $"{{\"Id\":\"{nodeId}\"}}",
                Partition,
                versionDetail);

            var result = factory.GetNode(triple);

            Assert.AreEqual(nodeId, result.Id);
            Assert.AreEqual(versionDetail, result.Version);
        }

        [TestMethod]
        public void GetNodeThrowsWhenVersionDetailIsNull()
        {
            var factory = CreateFactory();
            var nodeId = GlobalId.Get<TestNode>("test-id");
            var triple = new RDFTriple(
                nodeId,
                $"{GraphName}#type#TestNode#{nodeId}",
                " ",
                $"{{\"Id\":\"{nodeId}\"}}",
                Partition,
                null);

            Assert.ThrowsException<GraphlessDBOperationException>(() =>
            {
                factory.GetNode(triple);
            });
        }

        [TestMethod]
        public void GetEdgeReturnsDeserializedEdgeForHasInEdge()
        {
            var typeService = new MockGraphEntityTypeService();
            typeService.RegisterType<TestEdge>("TestEdge");
            var factory = CreateFactory(typeService: typeService);
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var predicate = new HasInEdge(GraphName, "TestNode", "TestEdge", inId, outId).ToString();
            var triple = new RDFTriple(
                inId,
                predicate,
                " ",
                $"{{\"InId\":\"{inId}\",\"OutId\":\"{outId}\"}}",
                Partition,
                null);

            var result = factory.GetEdge(triple);

            Assert.AreEqual(inId, result.InId);
            Assert.AreEqual(outId, result.OutId);
        }

        [TestMethod]
        public void GetEdgeReturnsDeserializedEdgeForHasOutEdge()
        {
            var typeService = new MockGraphEntityTypeService();
            typeService.RegisterType<TestEdge>("TestEdge");
            var factory = CreateFactory(typeService: typeService);
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var predicate = new HasOutEdge(GraphName, "TestNode", "TestEdge", inId, outId).ToString();
            var triple = new RDFTriple(
                outId,
                predicate,
                " ",
                $"{{\"InId\":\"{inId}\",\"OutId\":\"{outId}\"}}",
                Partition,
                null);

            var result = factory.GetEdge(triple);

            Assert.AreEqual(inId, result.InId);
            Assert.AreEqual(outId, result.OutId);
        }

        [TestMethod]
        public void GetEdgeThrowsForInvalidPredicate()
        {
            var factory = CreateFactory();
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var triple = new RDFTriple(
                inId,
                "InvalidPredicate",
                " ",
                $"{{\"InId\":\"{inId}\",\"OutId\":\"{outId}\"}}",
                Partition,
                null);

            Assert.ThrowsException<GraphlessDBOperationException>(() =>
            {
                factory.GetEdge(triple);
            });
        }

        [TestMethod]
        public void GetHasTypeRDFTripleReturnsHasType()
        {
            var factory = CreateFactory();
            var node = new TestNode(GlobalId.Get<TestNode>("test-id"));

            var result = factory.GetHasTypeRDFTriple(node);

            Assert.AreEqual(node.Id, result.Subject);
            Assert.IsTrue(result.Predicate.Contains("type", StringComparison.Ordinal));
        }

        [TestMethod]
        public void GetHasBlobRDFTripleReturnsHasBlob()
        {
            var factory = CreateFactory();
            var node = new TestNode(GlobalId.Get<TestNode>("test-id"));

            var result = factory.GetHasBlobRDFTriple(node);

            Assert.AreEqual(node.Id, result.Subject);
            Assert.IsTrue(result.Predicate.Contains("blob", StringComparison.Ordinal));
        }

        [TestMethod]
        public void GetHasEdgeRDFTriplesReturnsBothInAndOutEdges()
        {
            var factory = CreateFactory();
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdge(inId, outId);

            var result = factory.GetHasEdgeRDFTriples(edge);

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(inId, result[0].Subject);
            Assert.AreEqual(outId, result[1].Subject);
        }

        [TestMethod]
        public void GetHasEdgePropRDFTriplesReturnsEmptyForEdgeWithNoQueryableProperties()
        {
            var factory = CreateFactory();
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdge(inId, outId);

            var result = factory.GetHasEdgePropRDFTriples(edge);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void GetHasEdgePropRDFTriplesReturnsInAndOutForQueryableProperties()
        {
            var queryablePropertyService = new MockGraphQueryablePropertyService();
            queryablePropertyService.RegisterQueryableProperty("TestEdgeWithProperty", "Label");
            var factory = CreateFactory(queryablePropertyService: queryablePropertyService);
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdgeWithProperty(inId, outId, "TestLabel");

            var result = factory.GetHasEdgePropRDFTriples(edge);

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(inId, result[0].Subject);
            Assert.AreEqual(outId, result[1].Subject);
            Assert.AreEqual("TestLabel", result[0].Object);
            Assert.AreEqual("TestLabel", result[1].Object);
        }

        [TestMethod]
        public void GetHasEdgePropRDFTriplesExcludesDeletedAt()
        {
            var queryablePropertyService = new MockGraphQueryablePropertyService();
            queryablePropertyService.RegisterQueryableProperty("TestDeletedEdge", "DeletedAt");
            var factory = CreateFactory(queryablePropertyService: queryablePropertyService);
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 12, 31, 23, 59, 59, DateTimeKind.Utc);
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestDeletedEdge(inId, outId, createdAt, deletedAt);

            var result = factory.GetHasEdgePropRDFTriples(edge);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void GetHasEdgePropRDFTriplesExcludesInId()
        {
            var queryablePropertyService = new MockGraphQueryablePropertyService();
            queryablePropertyService.RegisterQueryableProperty("TestEdge", "InId");
            var factory = CreateFactory(queryablePropertyService: queryablePropertyService);
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdge(inId, outId);

            var result = factory.GetHasEdgePropRDFTriples(edge);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void GetHasEdgePropRDFTriplesExcludesOutId()
        {
            var queryablePropertyService = new MockGraphQueryablePropertyService();
            queryablePropertyService.RegisterQueryableProperty("TestEdge", "OutId");
            var factory = CreateFactory(queryablePropertyService: queryablePropertyService);
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdge(inId, outId);

            var result = factory.GetHasEdgePropRDFTriples(edge);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void GetHasEdgePropRDFTriplesExcludesNullProperties()
        {
            var queryablePropertyService = new MockGraphQueryablePropertyService();
            queryablePropertyService.RegisterQueryableProperty("TestEdgeWithProperty", "Label");
            var factory = CreateFactory(queryablePropertyService: queryablePropertyService);
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestEdgeWithProperty(inId, outId, null!);

            var result = factory.GetHasEdgePropRDFTriples(edge);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void GetHadEdgeRDFTriplesReturnsBothInAndOutEdges()
        {
            var factory = CreateFactory();
            var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var deletedAt = new DateTime(2024, 12, 31, 23, 59, 59, DateTimeKind.Utc);
            var inId = GlobalId.Get<TestNode>("in-id");
            var outId = GlobalId.Get<TestNode>("out-id");
            var edge = new TestDeletedEdge(inId, outId, createdAt, deletedAt);

            var result = factory.GetHadEdgeRDFTriples(edge);

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(inId, result[0].Subject);
            Assert.AreEqual(outId, result[1].Subject);
        }

        [TestMethod]
        public void GetHasPropRDFTriplesReturnsEmptyForNodeWithNoQueryableProperties()
        {
            var factory = CreateFactory();
            var node = new TestNode(GlobalId.Get<TestNode>("test-id"));

            var result = factory.GetHasPropRDFTriples(node);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void GetHasPropRDFTriplesReturnsTripleForQueryableProperty()
        {
            var queryablePropertyService = new MockGraphQueryablePropertyService();
            queryablePropertyService.RegisterQueryableProperty("TestNodeWithProperty", "Name");
            var factory = CreateFactory(queryablePropertyService: queryablePropertyService);
            var node = new TestNodeWithProperty(GlobalId.Get<TestNodeWithProperty>("test-id"), "TestName");

            var result = factory.GetHasPropRDFTriples(node);

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(node.Id, result[0].Subject);
            Assert.AreEqual("TestName", result[0].Object);
        }

        [TestMethod]
        public void GetHasPropRDFTriplesExcludesDeletedAt()
        {
            var queryablePropertyService = new MockGraphQueryablePropertyService();
            queryablePropertyService.RegisterQueryableProperty("TestNode", "DeletedAt");
            var factory = CreateFactory(queryablePropertyService: queryablePropertyService);
            var node = new TestNode(GlobalId.Get<TestNode>("test-id"));

            var result = factory.GetHasPropRDFTriples(node);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void GetHasPropRDFTriplesExcludesVersion()
        {
            var queryablePropertyService = new MockGraphQueryablePropertyService();
            queryablePropertyService.RegisterQueryableProperty("TestNode", "Version");
            var factory = CreateFactory(queryablePropertyService: queryablePropertyService);
            var node = new TestNode(GlobalId.Get<TestNode>("test-id"));

            var result = factory.GetHasPropRDFTriples(node);

            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void GetHasPropRDFTriplesExcludesNullProperties()
        {
            var queryablePropertyService = new MockGraphQueryablePropertyService();
            queryablePropertyService.RegisterQueryableProperty("TestNodeWithProperty", "Name");
            var factory = CreateFactory(queryablePropertyService: queryablePropertyService);
            var node = new TestNodeWithProperty(GlobalId.Get<TestNodeWithProperty>("test-id"), null!);

            var result = factory.GetHasPropRDFTriples(node);

            Assert.AreEqual(0, result.Count);
        }
    }
}
