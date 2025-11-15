/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using GraphlessDB.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Graph.Services.Internal.Tests
{
    [TestClass]
    public sealed class GraphCursorJsonSerializerContextTests
    {
        [TestMethod]
        public void CanSerializeEmptyCursor()
        {
            var cursor = new Cursor(ImmutableTree<string, CursorNode>.Empty);
            var json = JsonSerializer.Serialize(cursor, GraphCursorJsonSerializerContext.Default.Cursor);
            Assert.IsNotNull(json);
        }

        [TestMethod]
        public void CanDeserializeEmptyCursor()
        {
            var cursor = new Cursor(ImmutableTree<string, CursorNode>.Empty);
            var json = JsonSerializer.Serialize(cursor, GraphCursorJsonSerializerContext.Default.Cursor);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.Cursor);
            Assert.IsNotNull(deserialized);
        }

        [TestMethod]
        public void CanSerializeCursorWithNode()
        {
            var cursor = Cursor.Create(CursorNode.Empty);
            var json = JsonSerializer.Serialize(cursor, GraphCursorJsonSerializerContext.Default.Cursor);
            Assert.IsNotNull(json);
        }

        [TestMethod]
        public void CanDeserializeCursorWithNode()
        {
            var cursor = Cursor.Create(CursorNode.Empty);
            var json = JsonSerializer.Serialize(cursor, GraphCursorJsonSerializerContext.Default.Cursor);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.Cursor);
            Assert.IsNotNull(deserialized);
        }

        [TestMethod]
        public void CanSerializeCursorWithEndOfDataNode()
        {
            var cursor = Cursor.Create(CursorNode.EndOfDataNode);
            var json = JsonSerializer.Serialize(cursor, GraphCursorJsonSerializerContext.Default.Cursor);
            Assert.IsNotNull(json);
        }

        [TestMethod]
        public void CanDeserializeCursorWithEndOfDataNode()
        {
            var cursor = Cursor.Create(CursorNode.EndOfDataNode);
            var json = JsonSerializer.Serialize(cursor, GraphCursorJsonSerializerContext.Default.Cursor);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.Cursor);
            Assert.IsNotNull(deserialized);
        }

        [TestMethod]
        public void SerializationPreservesEmptyTree()
        {
            var cursor = new Cursor(ImmutableTree<string, CursorNode>.Empty);
            var json = JsonSerializer.Serialize(cursor, GraphCursorJsonSerializerContext.Default.Cursor);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.Cursor);
            Assert.IsNotNull(deserialized);
            Assert.AreEqual(cursor, deserialized);
        }

        [TestMethod]
        public void SerializationPreservesCursorWithNode()
        {
            var cursor = Cursor.Create(CursorNode.Empty);
            var json = JsonSerializer.Serialize(cursor, GraphCursorJsonSerializerContext.Default.Cursor);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.Cursor);
            Assert.IsNotNull(deserialized);
            Assert.AreEqual(cursor, deserialized);
        }

        [TestMethod]
        public void CanAccessCursorNodeTypeInfo()
        {
            JsonTypeInfo<CursorNode> typeInfo = GraphCursorJsonSerializerContext.Default.CursorNode;
            Assert.IsNotNull(typeInfo);
        }

        [TestMethod]
        public void CanSerializeCursorNodeDirectly()
        {
            var node = CursorNode.Empty;
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(json);
        }

        [TestMethod]
        public void CanDeserializeCursorNodeDirectly()
        {
            var node = CursorNode.Empty;
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(deserialized);
            Assert.AreEqual(node, deserialized);
        }

        [TestMethod]
        public void CanSerializeCursorNodeWithHasType()
        {
            var hasTypeCursor = new HasTypeCursor("subject", "partition", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var node = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(json);
        }

        [TestMethod]
        public void CanDeserializeCursorNodeWithHasType()
        {
            var hasTypeCursor = new HasTypeCursor("subject", "partition", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var node = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(deserialized);
            Assert.AreEqual(node, deserialized);
        }

        [TestMethod]
        public void CanSerializeCursorNodeWithHasProp()
        {
            var hasPropCursor = new HasPropCursor("subject", "propValue", "partition", ImmutableList<HasPropCursorQueryCursor>.Empty);
            var node = new CursorNode(null, hasPropCursor, null, null, null, null, null, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(json);
        }

        [TestMethod]
        public void CanDeserializeCursorNodeWithHasProp()
        {
            var hasPropCursor = new HasPropCursor("subject", "propValue", "partition", ImmutableList<HasPropCursorQueryCursor>.Empty);
            var node = new CursorNode(null, hasPropCursor, null, null, null, null, null, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(deserialized);
            Assert.AreEqual(node, deserialized);
        }

        [TestMethod]
        public void CanSerializeCursorNodeWithHasInEdge()
        {
            var hasInEdgeCursor = new HasInEdgeCursor("subject", "edgeTypeName", "nodeOutId");
            var node = new CursorNode(null, null, hasInEdgeCursor, null, null, null, null, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(json);
        }

        [TestMethod]
        public void CanDeserializeCursorNodeWithHasInEdge()
        {
            var hasInEdgeCursor = new HasInEdgeCursor("subject", "edgeTypeName", "nodeOutId");
            var node = new CursorNode(null, null, hasInEdgeCursor, null, null, null, null, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(deserialized);
            Assert.AreEqual(node, deserialized);
        }

        [TestMethod]
        public void CanSerializeCursorNodeWithHasInEdgeProp()
        {
            var hasInEdgePropCursor = new HasInEdgePropCursor("subject", "edgeTypeName", "nodeOutId", "propertyValue");
            var node = new CursorNode(null, null, null, hasInEdgePropCursor, null, null, null, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(json);
        }

        [TestMethod]
        public void CanDeserializeCursorNodeWithHasInEdgeProp()
        {
            var hasInEdgePropCursor = new HasInEdgePropCursor("subject", "edgeTypeName", "nodeOutId", "propertyValue");
            var node = new CursorNode(null, null, null, hasInEdgePropCursor, null, null, null, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(deserialized);
            Assert.AreEqual(node, deserialized);
        }

        [TestMethod]
        public void CanSerializeCursorNodeWithHasOutEdge()
        {
            var hasOutEdgeCursor = new HasOutEdgeCursor("subject", "edgeTypeName", "nodeInId");
            var node = new CursorNode(null, null, null, null, hasOutEdgeCursor, null, null, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(json);
        }

        [TestMethod]
        public void CanDeserializeCursorNodeWithHasOutEdge()
        {
            var hasOutEdgeCursor = new HasOutEdgeCursor("subject", "edgeTypeName", "nodeInId");
            var node = new CursorNode(null, null, null, null, hasOutEdgeCursor, null, null, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(deserialized);
            Assert.AreEqual(node, deserialized);
        }

        [TestMethod]
        public void CanSerializeCursorNodeWithHasOutEdgeProp()
        {
            var hasOutEdgePropCursor = new HasOutEdgePropCursor("subject", "edgeTypeName", "nodeInId", "propertyValue");
            var node = new CursorNode(null, null, null, null, null, hasOutEdgePropCursor, null, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(json);
        }

        [TestMethod]
        public void CanDeserializeCursorNodeWithHasOutEdgeProp()
        {
            var hasOutEdgePropCursor = new HasOutEdgePropCursor("subject", "edgeTypeName", "nodeInId", "propertyValue");
            var node = new CursorNode(null, null, null, null, null, hasOutEdgePropCursor, null, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(deserialized);
            Assert.AreEqual(node, deserialized);
        }

        [TestMethod]
        public void CanSerializeCursorNodeWithIndexed()
        {
            var indexedCursor = new IndexedCursor(0);
            var node = new CursorNode(null, null, null, null, null, null, indexedCursor, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(json);
        }

        [TestMethod]
        public void CanDeserializeCursorNodeWithIndexed()
        {
            var indexedCursor = new IndexedCursor(0);
            var node = new CursorNode(null, null, null, null, null, null, indexedCursor, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(deserialized);
            Assert.AreEqual(node, deserialized);
        }

        [TestMethod]
        public void DeserializeHandlesInvalidJsonGracefully()
        {
            Assert.ThrowsException<JsonException>(() =>
            {
                JsonSerializer.Deserialize("{invalid json}", GraphCursorJsonSerializerContext.Default.CursorNode);
            });
        }

        [TestMethod]
        public void DeserializeHandlesNullJsonGracefully()
        {
            var result = JsonSerializer.Deserialize("null", GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNull(result);
        }

        [TestMethod]
        public void SerializeHandlesComplexCursorNode()
        {
            var hasTypeCursor = new HasTypeCursor("subject1", "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var hasPropCursor = new HasPropCursor("subject2", "propValue2", "partition2", ImmutableList<HasPropCursorQueryCursor>.Empty);
            var node = new CursorNode(hasTypeCursor, hasPropCursor, null, null, null, null, null, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(deserialized);
            Assert.AreEqual(node, deserialized);
        }

        [TestMethod]
        public void SerializeHandlesAllCursorTypes()
        {
            var hasTypeCursor = new HasTypeCursor("s1", "p1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var hasPropCursor = new HasPropCursor("s2", "v2", "p2", ImmutableList<HasPropCursorQueryCursor>.Empty);
            var hasInEdgeCursor = new HasInEdgeCursor("s3", "e3", "n3");
            var hasInEdgePropCursor = new HasInEdgePropCursor("s4", "e4", "n4", "v4");
            var hasOutEdgeCursor = new HasOutEdgeCursor("s5", "e5", "n5");
            var hasOutEdgePropCursor = new HasOutEdgePropCursor("s6", "e6", "n6", "v6");
            var indexedCursor = new IndexedCursor(42);
            var node = new CursorNode(hasTypeCursor, hasPropCursor, hasInEdgeCursor, hasInEdgePropCursor, hasOutEdgeCursor, hasOutEdgePropCursor, indexedCursor, null);
            var json = JsonSerializer.Serialize(node, GraphCursorJsonSerializerContext.Default.CursorNode);
            var deserialized = JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.CursorNode);
            Assert.IsNotNull(deserialized);
            Assert.AreEqual(node, deserialized);
        }

        [TestMethod]
        public void DeserializeHandlesInvalidPropertyType()
        {
            // Test with a number instead of object for nested property
            var invalidJson = "{\"hasType\":123}";
            Assert.ThrowsException<JsonException>(() =>
            {
                JsonSerializer.Deserialize(invalidJson, GraphCursorJsonSerializerContext.Default.CursorNode);
            });
        }

        [TestMethod]
        public void DeserializeHandlesInvalidPropertyTypeForHasProp()
        {
            var invalidJson = "{\"hasProp\":123}";
            Assert.ThrowsException<JsonException>(() =>
            {
                JsonSerializer.Deserialize(invalidJson, GraphCursorJsonSerializerContext.Default.CursorNode);
            });
        }

        [TestMethod]
        public void DeserializeHandlesInvalidPropertyTypeForHasInEdge()
        {
            var invalidJson = "{\"hasInEdge\":123}";
            Assert.ThrowsException<JsonException>(() =>
            {
                JsonSerializer.Deserialize(invalidJson, GraphCursorJsonSerializerContext.Default.CursorNode);
            });
        }

        [TestMethod]
        public void DeserializeHandlesInvalidPropertyTypeForHasInEdgeProp()
        {
            var invalidJson = "{\"hasInEdgeProp\":123}";
            Assert.ThrowsException<JsonException>(() =>
            {
                JsonSerializer.Deserialize(invalidJson, GraphCursorJsonSerializerContext.Default.CursorNode);
            });
        }

        [TestMethod]
        public void DeserializeHandlesInvalidPropertyTypeForHasOutEdge()
        {
            var invalidJson = "{\"hasOutEdge\":123}";
            Assert.ThrowsException<JsonException>(() =>
            {
                JsonSerializer.Deserialize(invalidJson, GraphCursorJsonSerializerContext.Default.CursorNode);
            });
        }

        [TestMethod]
        public void DeserializeHandlesInvalidPropertyTypeForHasOutEdgeProp()
        {
            var invalidJson = "{\"hasOutEdgeProp\":123}";
            Assert.ThrowsException<JsonException>(() =>
            {
                JsonSerializer.Deserialize(invalidJson, GraphCursorJsonSerializerContext.Default.CursorNode);
            });
        }

        [TestMethod]
        public void DeserializeHandlesInvalidPropertyTypeForIndexed()
        {
            var invalidJson = "{\"indexed\":\"notAnObject\"}";
            Assert.ThrowsException<JsonException>(() =>
            {
                JsonSerializer.Deserialize(invalidJson, GraphCursorJsonSerializerContext.Default.CursorNode);
            });
        }

        [TestMethod]
        public void DeserializeHandlesInvalidPropertyTypeForEndOfData()
        {
            var invalidJson = "{\"endOfData\":123}";
            Assert.ThrowsException<JsonException>(() =>
            {
                JsonSerializer.Deserialize(invalidJson, GraphCursorJsonSerializerContext.Default.CursorNode);
            });
        }
    }
}
