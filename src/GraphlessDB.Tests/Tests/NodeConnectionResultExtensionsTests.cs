/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class NodeConnectionResultExtensionsTests
    {
        [TestMethod]
        public void EnsureValidReturnsSourceWhenBothCursorsAreValid()
        {
            var connection = new Connection<RelayEdge<INode>, INode>(
                System.Collections.Immutable.ImmutableList<RelayEdge<INode>>.Empty,
                new PageInfo(false, false, "start", "end"));
            
            var result = new NodeConnectionResult(
                ChildCursor: "child-cursor",
                Cursor: "cursor",
                NeedsMoreData: false,
                HasMoreData: false,
                Connection: connection);

            var validResult = result.EnsureValid();

            Assert.AreSame(result, validResult);
        }

        [TestMethod]
        public void EnsureValidThrowsWhenChildCursorIsEmpty()
        {
            var connection = new Connection<RelayEdge<INode>, INode>(
                System.Collections.Immutable.ImmutableList<RelayEdge<INode>>.Empty,
                new PageInfo(false, false, "start", "end"));
            
            var result = new NodeConnectionResult(
                ChildCursor: string.Empty,
                Cursor: "cursor",
                NeedsMoreData: false,
                HasMoreData: false,
                Connection: connection);

            var exception = Assert.ThrowsException<GraphlessDBOperationException>(() => result.EnsureValid());
            Assert.AreEqual("Empty ChildCursor was not expected", exception.Message);
        }

        [TestMethod]
        public void EnsureValidThrowsWhenCursorIsEmpty()
        {
            var connection = new Connection<RelayEdge<INode>, INode>(
                System.Collections.Immutable.ImmutableList<RelayEdge<INode>>.Empty,
                new PageInfo(false, false, "start", "end"));
            
            var result = new NodeConnectionResult(
                ChildCursor: "child-cursor",
                Cursor: string.Empty,
                NeedsMoreData: false,
                HasMoreData: false,
                Connection: connection);

            var exception = Assert.ThrowsException<GraphlessDBOperationException>(() => result.EnsureValid());
            Assert.AreEqual("Empty Cursor was not expected", exception.Message);
        }

        [TestMethod]
        public void EnsureValidReturnsSourceWhenChildCursorIsNull()
        {
            var connection = new Connection<RelayEdge<INode>, INode>(
                System.Collections.Immutable.ImmutableList<RelayEdge<INode>>.Empty,
                new PageInfo(false, false, "start", "end"));
            
            var result = new NodeConnectionResult(
                ChildCursor: null,
                Cursor: "cursor",
                NeedsMoreData: false,
                HasMoreData: false,
                Connection: connection);

            var validResult = result.EnsureValid();

            Assert.AreSame(result, validResult);
        }
    }
}
