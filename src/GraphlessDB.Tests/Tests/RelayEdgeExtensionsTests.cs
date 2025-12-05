/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using GraphlessDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class RelayEdgeExtensionsTests
    {
        [TestMethod]
        public void AsTypeConvertsRelayEdgeOfINodeToTypedNode()
        {
            var user = User.New("testuser");
            var cursor = "cursor123";
            var relayEdge = new RelayEdge<INode>(cursor, user);

            var result = relayEdge.AsType<User>();

            Assert.IsNotNull(result);
            Assert.AreEqual(cursor, result.Cursor);
            Assert.AreEqual(user.Id, result.Node.Id);
            Assert.AreEqual(user.Username, result.Node.Username);
        }

        [TestMethod]
        public void AsTypeConvertsRelayEdgeOfIEdgeToTypedEdge()
        {
            var edge = UserOwnsCarEdge.New("user1", "car1");
            var cursor = "cursor456";
            var relayEdge = new RelayEdge<IEdge>(cursor, edge);

            var result = relayEdge.AsType<UserOwnsCarEdge>();

            Assert.IsNotNull(result);
            Assert.AreEqual(cursor, result.Cursor);
            Assert.AreEqual(edge.InId, result.Node.InId);
            Assert.AreEqual(edge.OutId, result.Node.OutId);
        }

        [TestMethod]
        public void AsConnectionCreatesConnectionWithSingleEdge()
        {
            var user = User.New("testuser");
            var cursor = "cursor789";
            var relayEdge = new RelayEdge<User>(cursor, user);

            var result = relayEdge.AsConnection();

            Assert.IsNotNull(result);
            Assert.AreEqual(1, result.Edges.Count);
            Assert.AreEqual(relayEdge, result.Edges[0]);
            Assert.AreEqual(false, result.PageInfo.HasNextPage);
            Assert.AreEqual(false, result.PageInfo.HasPreviousPage);
            Assert.AreEqual(cursor, result.PageInfo.StartCursor);
            Assert.AreEqual(cursor, result.PageInfo.EndCursor);
        }

        [TestMethod]
        public void GetNullableCursorReturnsNullForEmptyCursor()
        {
            var user = User.New("testuser");
            var relayEdge = new RelayEdge<User>(string.Empty, user);

            var result = relayEdge.GetNullableCursor();

            Assert.IsNull(result);
        }

        [TestMethod]
        public void GetNullableCursorReturnsCursorForNonEmptyCursor()
        {
            var user = User.New("testuser");
            var cursor = "cursor123";
            var relayEdge = new RelayEdge<User>(cursor, user);

            var result = relayEdge.GetNullableCursor();

            Assert.AreEqual(cursor, result);
        }

        [TestMethod]
        public void TryGetStartCursorReturnsFirstCursor()
        {
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var user3 = User.New("user3");
            var edges = new List<RelayEdge<User>>
            {
                new RelayEdge<User>("cursor1", user1),
                new RelayEdge<User>("cursor2", user2),
                new RelayEdge<User>("cursor3", user3)
            };

            var result = edges.TryGetStartCursor();

            Assert.AreEqual("cursor1", result);
        }

        [TestMethod]
        public void TryGetStartCursorReturnsNullForEmptyList()
        {
            var edges = new List<RelayEdge<User>>();

            var result = edges.TryGetStartCursor();

            Assert.IsNull(result);
        }

        [TestMethod]
        public void TryGetEndCursorReturnsLastCursor()
        {
            var user1 = User.New("user1");
            var user2 = User.New("user2");
            var user3 = User.New("user3");
            var edges = new List<RelayEdge<User>>
            {
                new RelayEdge<User>("cursor1", user1),
                new RelayEdge<User>("cursor2", user2),
                new RelayEdge<User>("cursor3", user3)
            };

            var result = edges.TryGetEndCursor();

            Assert.AreEqual("cursor3", result);
        }

        [TestMethod]
        public void TryGetEndCursorReturnsNullForEmptyList()
        {
            var edges = new List<RelayEdge<User>>();

            var result = edges.TryGetEndCursor();

            Assert.IsNull(result);
        }

        [TestMethod]
        public void TryGetStartCursorReturnsSameCursorForSingleEdge()
        {
            var user = User.New("user");
            var edges = new List<RelayEdge<User>>
            {
                new RelayEdge<User>("cursor1", user)
            };

            var result = edges.TryGetStartCursor();

            Assert.AreEqual("cursor1", result);
        }

        [TestMethod]
        public void TryGetEndCursorReturnsSameCursorForSingleEdge()
        {
            var user = User.New("user");
            var edges = new List<RelayEdge<User>>
            {
                new RelayEdge<User>("cursor1", user)
            };

            var result = edges.TryGetEndCursor();

            Assert.AreEqual("cursor1", result);
        }
    }
}
