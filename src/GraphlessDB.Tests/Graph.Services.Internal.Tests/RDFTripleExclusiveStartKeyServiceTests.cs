/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using GraphlessDB.Graph;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Graph.Services.Internal.Tests
{
    [TestClass]
    public sealed class RDFTripleExclusiveStartKeyServiceTests
    {
        private static RDFTripleExclusiveStartKeyService CreateService()
        {
            var options = Options.Create(new GraphOptions
            {
                TableName = "TestTable",
                GraphName = "TestGraph",
                PartitionCount = 10
            });
            var graphSettingsService = new GraphDBSettingsService(options);
            var cursorSerializer = new GraphCursorSerializationService();
            var entityValueSerializer = new GraphSerializationService();
            return new RDFTripleExclusiveStartKeyService(graphSettingsService, cursorSerializer, entityValueSerializer);
        }

        [TestMethod]
        public void TryGetHasTypeCursorReturnsNullWhenPageIsNull()
        {
            var service = CreateService();
            var result = service.TryGetHasTypeCursor(null!);
            Assert.IsNull(result);
        }

        [TestMethod]
        public void TryGetHasTypeCursorReturnsNullWhenAfterAndBeforeAreNull()
        {
            var service = CreateService();
            var page = ConnectionArguments.GetFirst(10);
            var result = service.TryGetHasTypeCursor(page);
            Assert.IsNull(result);
        }

        [TestMethod]
        public void TryGetHasTypeCursorReturnsHasTypeCursorWhenAfterIsProvided()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var hasTypeCursor = new HasTypeCursor("subject1", "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            var result = service.TryGetHasTypeCursor(page);

            Assert.IsNotNull(result);
            Assert.AreEqual("subject1", result.Subject);
            Assert.AreEqual("partition1", result.Partition);
        }

        [TestMethod]
        public void TryGetHasTypeCursorReturnsHasTypeCursorWhenBeforeIsProvided()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var hasTypeCursor = new HasTypeCursor("subject2", "partition2", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetLast(10, cursorText);
            var result = service.TryGetHasTypeCursor(page);

            Assert.IsNotNull(result);
            Assert.AreEqual("subject2", result.Subject);
            Assert.AreEqual("partition2", result.Partition);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void TryGetHasTypeCursorThrowsWhenCursorTypeNotSupported()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var cursorNode = new CursorNode(null, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            service.TryGetHasTypeCursor(page);
        }

        [TestMethod]
        public void TryGetHasTypeExclusiveStartKeyReturnsNullWhenPageIsNull()
        {
            var service = CreateService();
            var result = service.TryGetHasTypeExclusiveStartKey(null, 0, "typeName");
            Assert.IsNull(result);
        }

        [TestMethod]
        public void TryGetHasTypeExclusiveStartKeyReturnsNullWhenAfterAndBeforeAreNull()
        {
            var service = CreateService();
            var page = ConnectionArguments.GetFirst(10);
            var result = service.TryGetHasTypeExclusiveStartKey(page, 0, "typeName");
            Assert.IsNull(result);
        }

        [TestMethod]
        public void TryGetHasTypeExclusiveStartKeyReturnsNullWhenPositionIsStart()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var queryCursor = new HasTypeCursorQueryCursor(0, PartitionPosition.Start, null);
            var hasTypeCursor = new HasTypeCursor("subject1", "partition1", ImmutableList.Create(queryCursor));
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            var result = service.TryGetHasTypeExclusiveStartKey(page, 0, "typeName");

            Assert.IsNull(result);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void TryGetHasTypeExclusiveStartKeyThrowsWhenPositionIsFinish()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var queryCursor = new HasTypeCursorQueryCursor(0, PartitionPosition.Finish, null);
            var hasTypeCursor = new HasTypeCursor("subject1", "partition1", ImmutableList.Create(queryCursor));
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            service.TryGetHasTypeExclusiveStartKey(page, 0, "typeName");
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void TryGetHasTypeExclusiveStartKeyThrowsWhenCursorIsNull()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var queryCursor = new HasTypeCursorQueryCursor(0, PartitionPosition.Cursor, null);
            var hasTypeCursor = new HasTypeCursor("subject1", "partition1", ImmutableList.Create(queryCursor));
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            service.TryGetHasTypeExclusiveStartKey(page, 0, "typeName");
        }

        [TestMethod]
        public void TryGetHasTypeExclusiveStartKeyReturnsKeyWhenCursorIsValid()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var partitionCursor = new HasTypeCursorPartitionCursor("subject1", "partition1");
            var queryCursor = new HasTypeCursorQueryCursor(0, PartitionPosition.Cursor, partitionCursor);
            var hasTypeCursor = new HasTypeCursor("subject1", "partition1", ImmutableList.Create(queryCursor));
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            var result = service.TryGetHasTypeExclusiveStartKey(page, 0, "Person");

            Assert.IsNotNull(result);
            Assert.AreEqual("subject1", result.Subject);
            Assert.AreEqual("partition1", result.Partition);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void TryGetHasTypeExclusiveStartKeyThrowsWhenCursorTypeNotSupported()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var cursorNode = new CursorNode(null, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            service.TryGetHasTypeExclusiveStartKey(page, 0, "typeName");
        }

        [TestMethod]
        public void TryGetHasEdgeExclusiveStartKeyReturnsNullWhenConnectionArgumentsIsNull()
        {
            var service = CreateService();
            var options = new GraphSettings("table", "graph", 10, "byPred", "byObj");
            var request = new ToEdgeQueryRequest("NodeType", "EdgeType", null!, null, null, null, false);

            var result = service.TryGetHasEdgeExclusiveStartKey(null, options, request);
            Assert.IsNull(result);
        }

        [TestMethod]
        public void TryGetHasEdgeExclusiveStartKeyReturnsNullWhenAfterAndBeforeAreNull()
        {
            var service = CreateService();
            var options = new GraphSettings("table", "graph", 10, "byPred", "byObj");
            var connectionArgs = ConnectionArguments.GetFirst(10);
            var request = new ToEdgeQueryRequest("NodeType", "EdgeType", null!, null, null, connectionArgs, false);

            var result = service.TryGetHasEdgeExclusiveStartKey(connectionArgs, options, request);
            Assert.IsNull(result);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void TryGetHasEdgeExclusiveStartKeyThrowsWhenEdgeTypeNameIsNull()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();
            var options = new GraphSettings("table", "graph", 10, "byPred", "byObj");

            var cursorNode = new CursorNode(null, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);
            var connectionArgs = ConnectionArguments.GetFirst(10, cursorText);

            var request = new ToEdgeQueryRequest("NodeType", null, null!, null, null, connectionArgs, false);
            service.TryGetHasEdgeExclusiveStartKey(connectionArgs, options, request);
        }

        [TestMethod]
        public void TryGetHasEdgeExclusiveStartKeyReturnsKeyForHasInEdge()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();
            var options = new GraphSettings("table", "graph", 10, "byPred", "byObj");

            var hasInEdgeCursor = new HasInEdgeCursor("subject1", "EdgeType", "nodeOut1");
            var cursorNode = new CursorNode(null, null, hasInEdgeCursor, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);
            var connectionArgs = ConnectionArguments.GetFirst(10, cursorText);

            var request = new ToEdgeQueryRequest("NodeType", "EdgeType", null!, null, null, connectionArgs, false);
            var result = service.TryGetHasEdgeExclusiveStartKey(connectionArgs, options, request);

            Assert.IsNotNull(result);
            Assert.AreEqual("subject1", result.Subject);
        }

        [TestMethod]
        public void TryGetHasEdgeExclusiveStartKeyReturnsKeyForHasInEdgeProp()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();
            var options = new GraphSettings("table", "graph", 10, "byPred", "byObj");

            var hasInEdgePropCursor = new HasInEdgePropCursor("subject1", "EdgeType", "nodeOut1", "propValue");
            var cursorNode = new CursorNode(null, null, null, hasInEdgePropCursor, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);
            var connectionArgs = ConnectionArguments.GetFirst(10, cursorText);

            var orderBy = new OrderArguments("PropertyName", OrderDirection.Asc);
            var request = new ToEdgeQueryRequest("NodeType", "EdgeType", null!, orderBy, null, connectionArgs, false);
            var result = service.TryGetHasEdgeExclusiveStartKey(connectionArgs, options, request);

            Assert.IsNotNull(result);
            Assert.AreEqual("subject1", result.Subject);
        }

        [TestMethod]
        public void TryGetHasEdgeExclusiveStartKeyReturnsKeyForHasOutEdge()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();
            var options = new GraphSettings("table", "graph", 10, "byPred", "byObj");

            var hasOutEdgeCursor = new HasOutEdgeCursor("subject1", "EdgeType", "nodeIn1");
            var cursorNode = new CursorNode(null, null, null, null, hasOutEdgeCursor, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);
            var connectionArgs = ConnectionArguments.GetFirst(10, cursorText);

            var request = new ToEdgeQueryRequest("NodeType", "EdgeType", null!, null, null, connectionArgs, false);
            var result = service.TryGetHasEdgeExclusiveStartKey(connectionArgs, options, request);

            Assert.IsNotNull(result);
            Assert.AreEqual("subject1", result.Subject);
        }

        [TestMethod]
        public void TryGetHasEdgeExclusiveStartKeyReturnsKeyForHasOutEdgeProp()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();
            var options = new GraphSettings("table", "graph", 10, "byPred", "byObj");

            var hasOutEdgePropCursor = new HasOutEdgePropCursor("subject1", "EdgeType", "nodeIn1", "propValue");
            var cursorNode = new CursorNode(null, null, null, null, null, hasOutEdgePropCursor, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);
            var connectionArgs = ConnectionArguments.GetFirst(10, cursorText);

            var filterBy = new EdgeFilterArguments("PropertyName", PropertyOperator.Equals, "filterValue");
            var request = new ToEdgeQueryRequest("NodeType", "EdgeType", null!, null, filterBy, connectionArgs, false);
            var result = service.TryGetHasEdgeExclusiveStartKey(connectionArgs, options, request);

            Assert.IsNotNull(result);
            Assert.AreEqual("subject1", result.Subject);
        }

        [TestMethod]
        public void TryGetHasEdgeExclusiveStartKeyReturnsNullForEndOfData()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();
            var options = new GraphSettings("table", "graph", 10, "byPred", "byObj");

            var cursorNode = CursorNode.CreateEndOfData();
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);
            var connectionArgs = ConnectionArguments.GetFirst(10, cursorText);

            var request = new ToEdgeQueryRequest("NodeType", "EdgeType", null!, null, null, connectionArgs, false);
            var result = service.TryGetHasEdgeExclusiveStartKey(connectionArgs, options, request);

            Assert.IsNull(result);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void TryGetHasEdgeExclusiveStartKeyThrowsWhenCursorTypeNotSupported()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();
            var options = new GraphSettings("table", "graph", 10, "byPred", "byObj");

            var hasTypeCursor = new HasTypeCursor("subject1", "partition1", ImmutableList<HasTypeCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(hasTypeCursor, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);
            var connectionArgs = ConnectionArguments.GetFirst(10, cursorText);

            var request = new ToEdgeQueryRequest("NodeType", "EdgeType", null!, null, null, connectionArgs, false);
            service.TryGetHasEdgeExclusiveStartKey(connectionArgs, options, request);
        }

        [TestMethod]
        public void TryGetHasPropCursorReturnsNullWhenAfterAndBeforeAreNull()
        {
            var service = CreateService();
            var page = ConnectionArguments.GetFirst(10);
            var result = service.TryGetHasPropCursor(page);
            Assert.IsNull(result);
        }

        [TestMethod]
        public void TryGetHasPropCursorReturnsHasPropCursorWhenAfterIsProvided()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var hasPropCursor = new HasPropCursor("subject1", "value1", "partition1", ImmutableList<HasPropCursorQueryCursor>.Empty);
            var cursorNode = new CursorNode(null, hasPropCursor, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            var result = service.TryGetHasPropCursor(page);

            Assert.IsNotNull(result);
            Assert.AreEqual("subject1", result.Subject);
            Assert.AreEqual("value1", result.PropertyValue);
            Assert.AreEqual("partition1", result.Partition);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void TryGetHasPropCursorThrowsWhenCursorTypeNotSupported()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var cursorNode = new CursorNode(null, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            service.TryGetHasPropCursor(page);
        }

        [TestMethod]
        public void TryGetPropertiesByTypeAndPropertyNameExclusiveStartKeyReturnsNullWhenAfterAndBeforeAreNull()
        {
            var service = CreateService();
            var page = ConnectionArguments.GetFirst(10);
            var result = service.TryGetPropertiesByTypeAndPropertyNameExclusiveStartKey(page, 0, "typeName", "propName");
            Assert.IsNull(result);
        }

        [TestMethod]
        public void TryGetPropertiesByTypeAndPropertyNameExclusiveStartKeyReturnsNullWhenPositionIsStart()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var queryCursor = new HasPropCursorQueryCursor(0, PartitionPosition.Start, null);
            var hasPropCursor = new HasPropCursor("subject1", "value1", "partition1", ImmutableList.Create(queryCursor));
            var cursorNode = new CursorNode(null, hasPropCursor, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            var result = service.TryGetPropertiesByTypeAndPropertyNameExclusiveStartKey(page, 0, "typeName", "propName");

            Assert.IsNull(result);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void TryGetPropertiesByTypeAndPropertyNameExclusiveStartKeyThrowsWhenPositionIsFinish()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var queryCursor = new HasPropCursorQueryCursor(0, PartitionPosition.Finish, null);
            var hasPropCursor = new HasPropCursor("subject1", "value1", "partition1", ImmutableList.Create(queryCursor));
            var cursorNode = new CursorNode(null, hasPropCursor, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            service.TryGetPropertiesByTypeAndPropertyNameExclusiveStartKey(page, 0, "typeName", "propName");
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void TryGetPropertiesByTypeAndPropertyNameExclusiveStartKeyThrowsWhenCursorIsNull()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var queryCursor = new HasPropCursorQueryCursor(0, PartitionPosition.Cursor, null);
            var hasPropCursor = new HasPropCursor("subject1", "value1", "partition1", ImmutableList.Create(queryCursor));
            var cursorNode = new CursorNode(null, hasPropCursor, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            service.TryGetPropertiesByTypeAndPropertyNameExclusiveStartKey(page, 0, "typeName", "propName");
        }

        [TestMethod]
        public void TryGetPropertiesByTypeAndPropertyNameExclusiveStartKeyReturnsKeyWhenCursorIsValid()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var partitionCursor = new HasPropCursorPartitionCursor("subject1", "value1", "partition1");
            var queryCursor = new HasPropCursorQueryCursor(0, PartitionPosition.Cursor, partitionCursor);
            var hasPropCursor = new HasPropCursor("subject1", "value1", "partition1", ImmutableList.Create(queryCursor));
            var cursorNode = new CursorNode(null, hasPropCursor, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            var result = service.TryGetPropertiesByTypeAndPropertyNameExclusiveStartKey(page, 0, "Person", "Name");

            Assert.IsNotNull(result);
            Assert.AreEqual("subject1", result.Subject);
            Assert.AreEqual("partition1", result.Partition);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void TryGetPropertiesByTypeAndPropertyNameExclusiveStartKeyThrowsWhenCursorTypeNotSupported()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var cursorNode = new CursorNode(null, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            service.TryGetPropertiesByTypeAndPropertyNameExclusiveStartKey(page, 0, "typeName", "propName");
        }

        [TestMethod]
        public void TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKeyReturnsNullWhenAfterAndBeforeAreNull()
        {
            var service = CreateService();
            var page = ConnectionArguments.GetFirst(10);
            var result = service.TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKey(page, 0, "typeName", "propName");
            Assert.IsNull(result);
        }

        [TestMethod]
        public void TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKeyReturnsNullWhenPositionIsStart()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var queryCursor = new HasPropCursorQueryCursor(0, PartitionPosition.Start, null);
            var hasPropCursor = new HasPropCursor("subject1", "value1", "partition1", ImmutableList.Create(queryCursor));
            var cursorNode = new CursorNode(null, hasPropCursor, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            var result = service.TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKey(page, 0, "typeName", "propName");

            Assert.IsNull(result);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKeyThrowsWhenPositionIsFinish()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var queryCursor = new HasPropCursorQueryCursor(0, PartitionPosition.Finish, null);
            var hasPropCursor = new HasPropCursor("subject1", "value1", "partition1", ImmutableList.Create(queryCursor));
            var cursorNode = new CursorNode(null, hasPropCursor, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            service.TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKey(page, 0, "typeName", "propName");
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKeyThrowsWhenCursorIsNull()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var queryCursor = new HasPropCursorQueryCursor(0, PartitionPosition.Cursor, null);
            var hasPropCursor = new HasPropCursor("subject1", "value1", "partition1", ImmutableList.Create(queryCursor));
            var cursorNode = new CursorNode(null, hasPropCursor, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            service.TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKey(page, 0, "typeName", "propName");
        }

        [TestMethod]
        public void TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKeyReturnsKeyWhenCursorIsValid()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var partitionCursor = new HasPropCursorPartitionCursor("subject1", "value1", "partition1");
            var queryCursor = new HasPropCursorQueryCursor(0, PartitionPosition.Cursor, partitionCursor);
            var hasPropCursor = new HasPropCursor("subject1", "value1", "partition1", ImmutableList.Create(queryCursor));
            var cursorNode = new CursorNode(null, hasPropCursor, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            var result = service.TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKey(page, 0, "Person", "Name");

            Assert.IsNotNull(result);
            Assert.AreEqual("subject1", result.Subject);
            Assert.AreEqual("partition1", result.Partition);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKeyThrowsWhenCursorTypeNotSupported()
        {
            var service = CreateService();
            var cursorSerializer = new GraphCursorSerializationService();

            var cursorNode = new CursorNode(null, null, null, null, null, null, null, null);
            var cursor = Cursor.Create(cursorNode);
            var cursorText = cursorSerializer.Serialize(cursor);

            var page = ConnectionArguments.GetFirst(10, cursorText);
            service.TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKey(page, 0, "typeName", "propName");
        }
    }
}
