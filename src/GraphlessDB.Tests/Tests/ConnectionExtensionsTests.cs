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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class ConnectionExtensionsTests
    {
        private static User CreateTestUser(string username)
        {
            return User.New(username);
        }

        private static Car CreateTestCar(string model)
        {
            return Car.New(model);
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

        #region AsType Tests

        [TestMethod]
        public void AsType_NodeConnection_ConvertsToTypedConnection()
        {
            // Arrange
            var user = CreateTestUser("testuser");
            var edge = CreateRelayEdge<INode>(user, "cursor1");
            var connection = CreateConnection(edge);

            // Act
            var result = connection.AsType<User>();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("cursor1", result.Edges[0].Cursor);
            Assert.AreEqual(user.Username, result.Edges[0].Node.Username);
        }

        [TestMethod]
        public void AsType_EdgeConnection_ConvertsToTypedConnection()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var edge = UserLikesUserEdge.New(user1, user2);
            var relayEdge = CreateRelayEdge<IEdge>(edge, "cursor1");
            var connection = new Connection<RelayEdge<IEdge>, IEdge>(
                [relayEdge],
                new PageInfo(false, false, "cursor1", "cursor1"));

            // Act
            var result = connection.AsType<UserLikesUserEdge>();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("cursor1", result.Edges[0].Cursor);
            Assert.AreEqual(edge.LikedByUsername, result.Edges[0].Node.LikedByUsername);
        }

        [TestMethod]
        public void AsType_EmptyConnection_ReturnsEmptyTypedConnection()
        {
            // Arrange
            var connection = new Connection<RelayEdge<INode>, INode>(
                ImmutableList<RelayEdge<INode>>.Empty,
                PageInfo.Empty);

            // Act
            var result = connection.AsType<User>();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Edges.Count);
        }

        #endregion

        #region FromCursorInclusive Tests

        [TestMethod]
        public void FromCursorInclusive_ValidCursor_ReturnsEdgesFromCursor()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var user3 = CreateTestUser("user3");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"),
                CreateRelayEdge(user3, "cursor3"));

            // Act
            var result = connection.FromCursorInclusive("cursor2");

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
            Assert.AreEqual("cursor2", result.Edges[0].Cursor);
            Assert.AreEqual("cursor3", result.Edges[1].Cursor);
        }

        [TestMethod]
        public void FromCursorInclusive_FirstCursor_ReturnsAllEdges()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));

            // Act
            var result = connection.FromCursorInclusive("cursor1");

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
        }

        [TestMethod]
        public void FromCursorInclusive_LastCursor_ReturnsLastEdge()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));

            // Act
            var result = connection.FromCursorInclusive("cursor2");

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("cursor2", result.Edges[0].Cursor);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void FromCursorInclusive_DuplicateCursors_ThrowsException()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var edge1 = CreateRelayEdge(user1, "cursor1");
            var edge2 = CreateRelayEdge(user2, "cursor1");
            var connection = new Connection<RelayEdge<User>, User>(
                [edge1, edge2],
                new PageInfo(false, false, "cursor1", "cursor1"));

            // Act
            connection.FromCursorInclusive("cursor1");
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void FromCursorInclusive_CursorNotFound_ThrowsException()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var connection = CreateConnection(CreateRelayEdge(user1, "cursor1"));

            // Act
            connection.FromCursorInclusive("nonexistent");
        }

        #endregion

        #region FromCursorExclusive Tests

        [TestMethod]
        public void FromCursorExclusive_ValidCursor_ReturnsEdgesAfterCursor()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var user3 = CreateTestUser("user3");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"),
                CreateRelayEdge(user3, "cursor3"));

            // Act
            var result = connection.FromCursorExclusive("cursor1");

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
            Assert.AreEqual("cursor2", result.Edges[0].Cursor);
            Assert.AreEqual("cursor3", result.Edges[1].Cursor);
        }

        [TestMethod]
        public void FromCursorExclusive_LastCursor_ReturnsEmpty()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));

            // Act
            var result = connection.FromCursorExclusive("cursor2");

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Edges.Count);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void FromCursorExclusive_DuplicateCursors_ThrowsException()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var edge1 = CreateRelayEdge(user1, "cursor1");
            var edge2 = CreateRelayEdge(user2, "cursor1");
            var connection = new Connection<RelayEdge<User>, User>(
                [edge1, edge2],
                new PageInfo(false, false, "cursor1", "cursor1"));

            // Act
            connection.FromCursorExclusive("cursor1");
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void FromCursorExclusive_CursorNotFound_ThrowsException()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var connection = CreateConnection(CreateRelayEdge(user1, "cursor1"));

            // Act
            connection.FromCursorExclusive("nonexistent");
        }

        #endregion

        #region ToCursorInclusive Tests

        [TestMethod]
        public void ToCursorInclusive_ValidCursor_ReturnsEdgesToCursor()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var user3 = CreateTestUser("user3");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"),
                CreateRelayEdge(user3, "cursor3"));

            // Act
            var result = connection.ToCursorInclusive("cursor2");

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
            Assert.AreEqual("cursor1", result.Edges[0].Cursor);
            Assert.AreEqual("cursor2", result.Edges[1].Cursor);
        }

        [TestMethod]
        public void ToCursorInclusive_FirstCursor_ReturnsFirstEdge()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));

            // Act
            var result = connection.ToCursorInclusive("cursor1");

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("cursor1", result.Edges[0].Cursor);
        }

        [TestMethod]
        public void ToCursorInclusive_LastCursor_ReturnsAllEdges()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));

            // Act
            var result = connection.ToCursorInclusive("cursor2");

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void ToCursorInclusive_DuplicateCursors_ThrowsException()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var edge1 = CreateRelayEdge(user1, "cursor1");
            var edge2 = CreateRelayEdge(user2, "cursor1");
            var connection = new Connection<RelayEdge<User>, User>(
                [edge1, edge2],
                new PageInfo(false, false, "cursor1", "cursor1"));

            // Act
            connection.ToCursorInclusive("cursor1");
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void ToCursorInclusive_CursorNotFound_ThrowsException()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var connection = CreateConnection(CreateRelayEdge(user1, "cursor1"));

            // Act
            connection.ToCursorInclusive("nonexistent");
        }

        #endregion

        #region ToCursorExclusive Tests

        [TestMethod]
        public void ToCursorExclusive_ValidCursor_ReturnsEdgesBeforeCursor()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var user3 = CreateTestUser("user3");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"),
                CreateRelayEdge(user3, "cursor3"));

            // Act
            var result = connection.ToCursorExclusive("cursor3");

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
            Assert.AreEqual("cursor1", result.Edges[0].Cursor);
            Assert.AreEqual("cursor2", result.Edges[1].Cursor);
        }

        [TestMethod]
        public void ToCursorExclusive_FirstCursor_ReturnsEmpty()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));

            // Act
            var result = connection.ToCursorExclusive("cursor1");

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Edges.Count);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void ToCursorExclusive_DuplicateCursors_ThrowsException()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var edge1 = CreateRelayEdge(user1, "cursor1");
            var edge2 = CreateRelayEdge(user2, "cursor1");
            var connection = new Connection<RelayEdge<User>, User>(
                [edge1, edge2],
                new PageInfo(false, false, "cursor1", "cursor1"));

            // Act
            connection.ToCursorExclusive("cursor1");
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void ToCursorExclusive_CursorNotFound_ThrowsException()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var connection = CreateConnection(CreateRelayEdge(user1, "cursor1"));

            // Act
            connection.ToCursorExclusive("nonexistent");
        }

        #endregion

        #region Single Tests

        [TestMethod]
        public void Single_ConnectionWithOneEdge_ReturnsSingleEdge()
        {
            // Arrange
            var user = CreateTestUser("testuser");
            var connection = CreateConnection(CreateRelayEdge(user, "cursor1"));

            // Act
            var result = connection.Single();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("testuser", result.Edges[0].Node.Username);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void Single_ConnectionWithMultipleEdges_ThrowsException()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));

            // Act
            connection.Single();
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void Single_EmptyConnection_ThrowsException()
        {
            // Arrange
            var connection = new Connection<RelayEdge<User>, User>(
                ImmutableList<RelayEdge<User>>.Empty,
                PageInfo.Empty);

            // Act
            connection.Single();
        }

        #endregion

        #region SingleOrDefault Tests

        [TestMethod]
        public void SingleOrDefault_ConnectionWithOneEdge_ReturnsSingleEdge()
        {
            // Arrange
            var user = CreateTestUser("testuser");
            var connection = CreateConnection(CreateRelayEdge(user, "cursor1"));

            // Act
            var result = connection.SingleOrDefault();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("testuser", result.Edges[0].Node.Username);
        }

        [TestMethod]
        public void SingleOrDefault_EmptyConnection_ReturnsEmptyConnection()
        {
            // Arrange
            var connection = new Connection<RelayEdge<User>, User>(
                ImmutableList<RelayEdge<User>>.Empty,
                PageInfo.Empty);

            // Act
            var result = connection.SingleOrDefault();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Edges.Count);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void SingleOrDefault_ConnectionWithMultipleEdges_ThrowsException()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));

            // Act
            connection.SingleOrDefault();
        }

        #endregion

        #region Select Tests

        [TestMethod]
        public void Select_WithSelector_TransformsNodes()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));

            // Act
            var result = connection.Select(edge => new RelayEdge<Car>(edge.Cursor, CreateTestCar(edge.Node.Username)));

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
            Assert.AreEqual("user1", result.Edges[0].Node.Model);
            Assert.AreEqual("user2", result.Edges[1].Node.Model);
        }

        [TestMethod]
        public void Select_WithIndexSelector_TransformsNodesWithIndex()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));

            // Act
            var result = connection.Select((edge, index) =>
                new RelayEdge<Car>(edge.Cursor, CreateTestCar($"{edge.Node.Username}_{index}")));

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
            Assert.AreEqual("user1_0", result.Edges[0].Node.Model);
            Assert.AreEqual("user2_1", result.Edges[1].Node.Model);
        }

        [TestMethod]
        public void Select_EmptyConnection_ReturnsEmptyConnection()
        {
            // Arrange
            var connection = new Connection<RelayEdge<User>, User>(
                ImmutableList<RelayEdge<User>>.Empty,
                PageInfo.Empty);

            // Act
            var result = connection.Select(edge => new RelayEdge<Car>(edge.Cursor, CreateTestCar("test")));

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Edges.Count);
        }

        #endregion

        #region First Tests

        [TestMethod]
        public void First_ConnectionWithEdges_ReturnsFirstEdge()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));

            // Act
            var result = connection.First();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("user1", result.Edges[0].Node.Username);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void First_EmptyConnection_ThrowsException()
        {
            // Arrange
            var connection = new Connection<RelayEdge<User>, User>(
                ImmutableList<RelayEdge<User>>.Empty,
                PageInfo.Empty);

            // Act
            connection.First();
        }

        #endregion

        #region FirstOrDefault Tests

        [TestMethod]
        public void FirstOrDefault_ConnectionWithEdges_ReturnsFirstEdge()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));

            // Act
            var result = connection.FirstOrDefault();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("user1", result.Edges[0].Node.Username);
        }

        [TestMethod]
        public void FirstOrDefault_EmptyConnection_ReturnsEmptyConnection()
        {
            // Arrange
            var connection = new Connection<RelayEdge<User>, User>(
                ImmutableList<RelayEdge<User>>.Empty,
                PageInfo.Empty);

            // Act
            var result = connection.FirstOrDefault();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Edges.Count);
        }

        #endregion

        #region Truncate Tests

        [TestMethod]
        public void Truncate_EdgeCountLessThanPageSize_ReturnsOriginalConnection()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));
            var page = ConnectionArguments.GetFirst(10);

            // Act
            var result = connection.Truncate(page);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
            Assert.IsFalse(result.PageInfo.HasNextPage);
        }

        [TestMethod]
        public void Truncate_EdgeCountExceedsPageSize_ReturnsTruncatedConnection()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var user3 = CreateTestUser("user3");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"),
                CreateRelayEdge(user3, "cursor3"));
            var page = ConnectionArguments.GetFirst(2);

            // Act
            var result = connection.Truncate(page);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
            Assert.AreEqual("cursor1", result.Edges[0].Cursor);
            Assert.AreEqual("cursor2", result.Edges[1].Cursor);
        }

        [TestMethod]
        public void Truncate_WithAfterCursor_SetsHasNextPageTrue()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var user3 = CreateTestUser("user3");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"),
                CreateRelayEdge(user3, "cursor3"));
            var page = ConnectionArguments.GetFirst(2, "cursor0");

            // Act
            var result = connection.Truncate(page);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
            Assert.IsTrue(result.PageInfo.HasNextPage);
        }

        [TestMethod]
        public void Truncate_WithBeforeCursor_SetsHasPreviousPageTrue()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var user3 = CreateTestUser("user3");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"),
                CreateRelayEdge(user3, "cursor3"));
            var page = ConnectionArguments.GetLast(2, "cursor5");

            // Act
            var result = connection.Truncate(page);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
            Assert.IsTrue(result.PageInfo.HasPreviousPage);
        }

        [TestMethod]
        public void Truncate_ExistingPageInfoHasNextPage_PreservesHasNextPage()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var user3 = CreateTestUser("user3");
            var connection = new Connection<RelayEdge<User>, User>(
                [CreateRelayEdge(user1, "cursor1"), CreateRelayEdge(user2, "cursor2"), CreateRelayEdge(user3, "cursor3")],
                new PageInfo(true, false, "cursor1", "cursor3"));
            var page = ConnectionArguments.GetFirst(2);

            // Act
            var result = connection.Truncate(page);

            // Assert
            Assert.IsNotNull(result);
            Assert.IsTrue(result.PageInfo.HasNextPage);
        }

        [TestMethod]
        public void Truncate_ExistingPageInfoHasPreviousPage_PreservesHasPreviousPage()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var user3 = CreateTestUser("user3");
            var connection = new Connection<RelayEdge<User>, User>(
                [CreateRelayEdge(user1, "cursor1"), CreateRelayEdge(user2, "cursor2"), CreateRelayEdge(user3, "cursor3")],
                new PageInfo(false, true, "cursor1", "cursor3"));
            var page = ConnectionArguments.GetFirst(2);

            // Act
            var result = connection.Truncate(page);

            // Assert
            Assert.IsNotNull(result);
            Assert.IsTrue(result.PageInfo.HasPreviousPage);
        }

        #endregion

        #region GetPagedConnection Tests

        [TestMethod]
        public void GetPagedConnection_NullPage_ReturnsOriginalConnection()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var connection = CreateConnection(CreateRelayEdge(user1, "cursor1"));

            // Act
            var result = connection.GetPagedConnection(null);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreSame(connection, result);
        }

        [TestMethod]
        public void GetPagedConnection_EmptyAfterAndBefore_ReturnsOriginalConnection()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var connection = CreateConnection(CreateRelayEdge(user1, "cursor1"));
            var page = ConnectionArguments.GetFirst(10);

            // Act
            var result = connection.GetPagedConnection(page);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreSame(connection, result);
        }

        [TestMethod]
        public void GetPagedConnection_WithAfterCursor_ReturnsEdgesAfterCursor()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var user3 = CreateTestUser("user3");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"),
                CreateRelayEdge(user3, "cursor3"));
            var page = ConnectionArguments.GetFirst(10, "cursor1");

            // Act
            var result = connection.GetPagedConnection(page);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(3, result.Edges.Count);
            Assert.AreEqual("cursor1", result.Edges[0].Cursor);
            Assert.AreEqual("cursor2", result.Edges[1].Cursor);
            Assert.AreEqual("cursor3", result.Edges[2].Cursor);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void GetPagedConnection_WithBeforeCursor_ThrowsNotSupportedException()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var connection = CreateConnection(CreateRelayEdge(user1, "cursor1"));
            var page = ConnectionArguments.GetLast(10, "cursor2");

            // Act
            connection.GetPagedConnection(page);
        }

        [TestMethod]
        public void GetPagedConnection_AfterCursorWithEmptyAfter_IncludesAllEdges()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));
            var page = ConnectionArguments.GetFirst(10, string.Empty);

            // Act
            var result = connection.GetPagedConnection(page);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
        }

        [TestMethod]
        public void GetPagedConnection_UpdatesStartAndEndCursors()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var user3 = CreateTestUser("user3");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"),
                CreateRelayEdge(user3, "cursor3"));
            var page = ConnectionArguments.GetFirst(10, "cursor1");

            // Act
            var result = connection.GetPagedConnection(page);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual("cursor1", result.PageInfo.StartCursor);
            Assert.AreEqual("cursor3", result.PageInfo.EndCursor);
        }

        #endregion

        #region ToConnection Tests

        [TestMethod]
        public void ToConnection_EnumerableOfEdges_CreatesConnection()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var edges = new[]
            {
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2")
            };

            // Act
            var result = edges.ToConnection();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Edges.Count);
            Assert.AreEqual("cursor1", result.PageInfo.StartCursor);
            Assert.AreEqual("cursor2", result.PageInfo.EndCursor);
            Assert.IsFalse(result.PageInfo.HasNextPage);
            Assert.IsFalse(result.PageInfo.HasPreviousPage);
        }

        [TestMethod]
        public void ToConnection_EmptyEnumerable_CreatesEmptyConnection()
        {
            // Arrange
            var edges = Enumerable.Empty<RelayEdge<User>>();

            // Act
            var result = edges.ToConnection();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Edges.Count);
            Assert.AreEqual(string.Empty, result.PageInfo.StartCursor);
            Assert.AreEqual(string.Empty, result.PageInfo.EndCursor);
        }

        [TestMethod]
        public void ToConnection_SingleEdge_CreatesConnectionWithSameCursors()
        {
            // Arrange
            var user = CreateTestUser("user1");
            var edges = new[] { CreateRelayEdge(user, "cursor1") };

            // Act
            var result = edges.ToConnection();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual("cursor1", result.PageInfo.StartCursor);
            Assert.AreEqual("cursor1", result.PageInfo.EndCursor);
        }

        #endregion

        #region ToImmutableNodeList Tests

        [TestMethod]
        public void ToImmutableNodeList_ConnectionWithNodes_ReturnsNodeList()
        {
            // Arrange
            var user1 = CreateTestUser("user1");
            var user2 = CreateTestUser("user2");
            var connection = CreateConnection(
                CreateRelayEdge(user1, "cursor1"),
                CreateRelayEdge(user2, "cursor2"));

            // Act
            var result = connection.ToImmutableNodeList();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("user1", result[0].Username);
            Assert.AreEqual("user2", result[1].Username);
        }

        [TestMethod]
        public void ToImmutableNodeList_EmptyConnection_ReturnsEmptyList()
        {
            // Arrange
            var connection = new Connection<RelayEdge<User>, User>(
                ImmutableList<RelayEdge<User>>.Empty,
                PageInfo.Empty);

            // Act
            var result = connection.ToImmutableNodeList();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Count);
        }

        #endregion
    }
}
