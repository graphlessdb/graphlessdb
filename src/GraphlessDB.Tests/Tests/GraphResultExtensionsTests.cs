/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Linq;
using GraphlessDB;
using GraphlessDB.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class GraphResultExtensionsTests
    {
        // Mock GraphResult type for testing unsupported cases
        private sealed record MockGraphResult(
            string? ChildCursor,
            string Cursor,
            bool NeedsMoreData,
            bool HasMoreData) : GraphResult(ChildCursor, Cursor, NeedsMoreData, HasMoreData);

        private static User CreateTestUser(string username)
        {
            return User.New(username);
        }

        private static Car CreateTestCar(string model)
        {
            return Car.New(model);
        }

        private static UserOwnsCarEdge CreateTestEdge(string inId, string outId)
        {
            return UserOwnsCarEdge.New(inId, outId);
        }

        private static RelayEdge<T> CreateRelayEdge<T>(T node, string cursor)
        {
            return new RelayEdge<T>(cursor, node);
        }

        private static Connection<RelayEdge<T>, T> CreateConnection<T>(params RelayEdge<T>[] edges)
        {
            var edgesList = edges.ToImmutableList();
            var pageInfo = new PageInfo(
                false,
                false,
                edgesList.FirstOrDefault()?.Cursor ?? string.Empty,
                edgesList.LastOrDefault()?.Cursor ?? string.Empty);
            return new Connection<RelayEdge<T>, T>(edgesList, pageInfo);
        }

        #region IsConnection Tests

        [TestMethod]
        public void IsConnectionReturnsTrueForNodeConnectionResult()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new NodeConnectionResult(null, "cursor", false, false, connection);

            var isConnection = result.IsConnection();

            Assert.IsTrue(isConnection);
        }

        [TestMethod]
        public void IsConnectionReturnsTrueForEdgeConnectionResult()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new EdgeConnectionResult(null, "cursor", false, false, connection);

            var isConnection = result.IsConnection();

            Assert.IsTrue(isConnection);
        }

        [TestMethod]
        public void IsConnectionReturnsFalseForNodeResult()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var result = new NodeResult(null, "cursor", false, false, relayEdge);

            var isConnection = result.IsConnection();

            Assert.IsFalse(isConnection);
        }

        [TestMethod]
        public void IsConnectionReturnsFalseForEdgeResult()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var result = new EdgeResult(null, "cursor", false, false, relayEdge);

            var isConnection = result.IsConnection();

            Assert.IsFalse(isConnection);
        }

        [TestMethod]
        public void IsConnectionThrowsNotSupportedExceptionForUnsupportedType()
        {
            var result = new MockGraphResult(null, "cursor", false, false);

            Assert.ThrowsException<NotSupportedException>(() => result.IsConnection());
        }

        #endregion

        #region GetConnection Tests

        [TestMethod]
        public void GetConnectionReturnsConnectionForNodeConnectionResult()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new NodeConnectionResult(null, "cursor", false, false, connection);

            var typedConnection = result.GetConnection<User>();

            Assert.IsNotNull(typedConnection);
            Assert.AreEqual(1, typedConnection.Edges.Count);
        }

        [TestMethod]
        public void GetConnectionReturnsConnectionForNodeResultWithNode()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var result = new NodeResult(null, "cursor", false, false, relayEdge);

            var typedConnection = result.GetConnection<User>();

            Assert.IsNotNull(typedConnection);
            Assert.AreEqual(1, typedConnection.Edges.Count);
        }

        [TestMethod]
        public void GetConnectionReturnsEmptyConnectionForNodeResultWithNullNode()
        {
            var result = new NodeResult(null, "cursor", false, false, null);

            var typedConnection = result.GetConnection<User>();

            Assert.IsNotNull(typedConnection);
            Assert.AreEqual(0, typedConnection.Edges.Count);
        }

        [TestMethod]
        public void GetConnectionThrowsNotSupportedExceptionForEdgeConnectionResult()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new EdgeConnectionResult(null, "cursor", false, false, connection);

            Assert.ThrowsException<NotSupportedException>(() => result.GetConnection<User>());
        }

        [TestMethod]
        public void GetConnectionThrowsNotSupportedExceptionForEdgeResult()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var result = new EdgeResult(null, "cursor", false, false, relayEdge);

            Assert.ThrowsException<NotSupportedException>(() => result.GetConnection<User>());
        }

        #endregion

        #region GetEdgeConnection Tests

        [TestMethod]
        public void GetEdgeConnectionReturnsConnectionForEdgeConnectionResult()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new EdgeConnectionResult(null, "cursor", false, false, connection);

            var typedConnection = result.GetEdgeConnection<UserOwnsCarEdge>();

            Assert.IsNotNull(typedConnection);
            Assert.AreEqual(1, typedConnection.Edges.Count);
        }

        [TestMethod]
        public void GetEdgeConnectionReturnsConnectionForEdgeResultWithEdge()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var result = new EdgeResult(null, "cursor", false, false, relayEdge);

            var typedConnection = result.GetEdgeConnection<UserOwnsCarEdge>();

            Assert.IsNotNull(typedConnection);
            Assert.AreEqual(1, typedConnection.Edges.Count);
        }

        [TestMethod]
        public void GetEdgeConnectionReturnsEmptyConnectionForEdgeResultWithNullEdge()
        {
            var result = new EdgeResult(null, "cursor", false, false, null);

            var typedConnection = result.GetEdgeConnection<UserOwnsCarEdge>();

            Assert.IsNotNull(typedConnection);
            Assert.AreEqual(0, typedConnection.Edges.Count);
        }

        [TestMethod]
        public void GetEdgeConnectionThrowsNotSupportedExceptionForNodeConnectionResult()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new NodeConnectionResult(null, "cursor", false, false, connection);

            Assert.ThrowsException<NotSupportedException>(() => result.GetEdgeConnection<UserOwnsCarEdge>());
        }

        [TestMethod]
        public void GetEdgeConnectionThrowsNotSupportedExceptionForNodeResult()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var result = new NodeResult(null, "cursor", false, false, relayEdge);

            Assert.ThrowsException<NotSupportedException>(() => result.GetEdgeConnection<UserOwnsCarEdge>());
        }

        #endregion

        #region GetPageInfo Tests

        [TestMethod]
        public void GetPageInfoReturnsPageInfoForNodeConnectionResult()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new NodeConnectionResult(null, "cursor", false, false, connection);

            var pageInfo = result.GetPageInfo();

            Assert.IsNotNull(pageInfo);
            Assert.AreEqual("cursor1", pageInfo.StartCursor);
            Assert.AreEqual("cursor1", pageInfo.EndCursor);
        }

        [TestMethod]
        public void GetPageInfoReturnsPageInfoForEdgeConnectionResult()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new EdgeConnectionResult(null, "cursor", false, false, connection);

            var pageInfo = result.GetPageInfo();

            Assert.IsNotNull(pageInfo);
            Assert.AreEqual("cursor1", pageInfo.StartCursor);
            Assert.AreEqual("cursor1", pageInfo.EndCursor);
        }

        [TestMethod]
        public void GetPageInfoThrowsNotSupportedExceptionForNodeResult()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var result = new NodeResult(null, "cursor", false, false, relayEdge);

            Assert.ThrowsException<NotSupportedException>(() => result.GetPageInfo());
        }

        [TestMethod]
        public void GetPageInfoThrowsNotSupportedExceptionForEdgeResult()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var result = new EdgeResult(null, "cursor", false, false, relayEdge);

            Assert.ThrowsException<NotSupportedException>(() => result.GetPageInfo());
        }

        #endregion

        #region TryGetRelayEdge Tests

        [TestMethod]
        public void TryGetRelayEdgeReturnsRelayEdgeForNodeResultWithNode()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var result = new NodeResult(null, "cursor", false, false, relayEdge);

            var edge = result.TryGetRelayEdge<User>();

            Assert.IsNotNull(edge);
            Assert.AreEqual("cursor1", edge.Cursor);
        }

        [TestMethod]
        public void TryGetRelayEdgeReturnsNullForNodeResultWithNullNode()
        {
            var result = new NodeResult(null, "cursor", false, false, null);

            var edge = result.TryGetRelayEdge<User>();

            Assert.IsNull(edge);
        }

        [TestMethod]
        public void TryGetRelayEdgeThrowsNotSupportedExceptionForNodeConnectionResult()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new NodeConnectionResult(null, "cursor", false, false, connection);

            Assert.ThrowsException<NotSupportedException>(() => result.TryGetRelayEdge<User>());
        }

        [TestMethod]
        public void TryGetRelayEdgeThrowsNotSupportedExceptionForEdgeConnectionResult()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new EdgeConnectionResult(null, "cursor", false, false, connection);

            Assert.ThrowsException<NotSupportedException>(() => result.TryGetRelayEdge<User>());
        }

        [TestMethod]
        public void TryGetRelayEdgeThrowsNotSupportedExceptionForEdgeResult()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var result = new EdgeResult(null, "cursor", false, false, relayEdge);

            Assert.ThrowsException<NotSupportedException>(() => result.TryGetRelayEdge<User>());
        }

        #endregion

        #region TryGetRelayEdgeEdge Tests

        [TestMethod]
        public void TryGetRelayEdgeEdgeReturnsRelayEdgeForEdgeResultWithEdge()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var result = new EdgeResult(null, "cursor", false, false, relayEdge);

            var resultEdge = result.TryGetRelayEdgeEdge<UserOwnsCarEdge>();

            Assert.IsNotNull(resultEdge);
            Assert.AreEqual("cursor1", resultEdge.Cursor);
        }

        [TestMethod]
        public void TryGetRelayEdgeEdgeReturnsNullForEdgeResultWithNullEdge()
        {
            var result = new EdgeResult(null, "cursor", false, false, null);

            var edge = result.TryGetRelayEdgeEdge<UserOwnsCarEdge>();

            Assert.IsNull(edge);
        }

        [TestMethod]
        public void TryGetRelayEdgeEdgeThrowsNotSupportedExceptionForNodeConnectionResult()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new NodeConnectionResult(null, "cursor", false, false, connection);

            Assert.ThrowsException<NotSupportedException>(() => result.TryGetRelayEdgeEdge<UserOwnsCarEdge>());
        }

        [TestMethod]
        public void TryGetRelayEdgeEdgeThrowsNotSupportedExceptionForEdgeConnectionResult()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new EdgeConnectionResult(null, "cursor", false, false, connection);

            Assert.ThrowsException<NotSupportedException>(() => result.TryGetRelayEdgeEdge<UserOwnsCarEdge>());
        }

        [TestMethod]
        public void TryGetRelayEdgeEdgeThrowsNotSupportedExceptionForNodeResult()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var result = new NodeResult(null, "cursor", false, false, relayEdge);

            Assert.ThrowsException<NotSupportedException>(() => result.TryGetRelayEdgeEdge<UserOwnsCarEdge>());
        }

        #endregion

        #region GetRelayEdge Tests

        [TestMethod]
        public void GetRelayEdgeReturnsRelayEdgeForNodeResultWithNode()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var result = new NodeResult(null, "cursor", false, false, relayEdge);

            var edge = result.GetRelayEdge<User>();

            Assert.IsNotNull(edge);
            Assert.AreEqual("cursor1", edge.Cursor);
        }

        [TestMethod]
        public void GetRelayEdgeThrowsGraphlessDBOperationExceptionForNodeResultWithNullNode()
        {
            var result = new NodeResult(null, "cursor", false, false, null);

            Assert.ThrowsException<GraphlessDBOperationException>(() => result.GetRelayEdge<User>());
        }

        [TestMethod]
        public void GetRelayEdgeThrowsNotSupportedExceptionForNodeConnectionResult()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new NodeConnectionResult(null, "cursor", false, false, connection);

            Assert.ThrowsException<NotSupportedException>(() => result.GetRelayEdge<User>());
        }

        [TestMethod]
        public void GetRelayEdgeThrowsNotSupportedExceptionForEdgeConnectionResult()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new EdgeConnectionResult(null, "cursor", false, false, connection);

            Assert.ThrowsException<NotSupportedException>(() => result.GetRelayEdge<User>());
        }

        [TestMethod]
        public void GetRelayEdgeThrowsNotSupportedExceptionForEdgeResult()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var result = new EdgeResult(null, "cursor", false, false, relayEdge);

            Assert.ThrowsException<NotSupportedException>(() => result.GetRelayEdge<User>());
        }

        #endregion

        #region GetRelayEdgeEdge Tests

        [TestMethod]
        public void GetRelayEdgeEdgeReturnsRelayEdgeForEdgeResultWithEdge()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var result = new EdgeResult(null, "cursor", false, false, relayEdge);

            var resultEdge = result.GetRelayEdgeEdge<UserOwnsCarEdge>();

            Assert.IsNotNull(resultEdge);
            Assert.AreEqual("cursor1", resultEdge.Cursor);
        }

        [TestMethod]
        public void GetRelayEdgeEdgeThrowsGraphlessDBOperationExceptionForEdgeResultWithNullEdge()
        {
            var result = new EdgeResult(null, "cursor", false, false, null);

            Assert.ThrowsException<GraphlessDBOperationException>(() => result.GetRelayEdgeEdge<UserOwnsCarEdge>());
        }

        [TestMethod]
        public void GetRelayEdgeEdgeThrowsNotSupportedExceptionForNodeConnectionResult()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new NodeConnectionResult(null, "cursor", false, false, connection);

            Assert.ThrowsException<NotSupportedException>(() => result.GetRelayEdgeEdge<UserOwnsCarEdge>());
        }

        [TestMethod]
        public void GetRelayEdgeEdgeThrowsNotSupportedExceptionForEdgeConnectionResult()
        {
            var edge = CreateTestEdge("in1", "out1");
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var connection = CreateConnection(relayEdge);
            var result = new EdgeConnectionResult(null, "cursor", false, false, connection);

            Assert.ThrowsException<NotSupportedException>(() => result.GetRelayEdgeEdge<UserOwnsCarEdge>());
        }

        [TestMethod]
        public void GetRelayEdgeEdgeThrowsNotSupportedExceptionForNodeResult()
        {
            var user = CreateTestUser("testuser");
            var relayEdge = CreateRelayEdge<INode>(user, "cursor1");
            var result = new NodeResult(null, "cursor", false, false, relayEdge);

            Assert.ThrowsException<NotSupportedException>(() => result.GetRelayEdgeEdge<UserOwnsCarEdge>());
        }

        #endregion
    }
}
