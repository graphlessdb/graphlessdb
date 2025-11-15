/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB;
using GraphlessDB.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class GraphQueryItemExtensionsTests
    {
        #region WithCursor Tests

        [TestMethod]
        public void WithCursorNodeConnectionQueryReturnsSameTypeWithUpdatedCursor()
        {
            var query = new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithCursor("test-cursor");

            Assert.IsInstanceOfType(result.Query, typeof(NodeConnectionQuery));
            var resultQuery = (NodeConnectionQuery)result.Query;
            Assert.AreEqual("test-cursor", resultQuery.Page.After);
        }

        [TestMethod]
        public void WithCursorInToEdgeConnectionQueryReturnsSameTypeWithUpdatedCursor()
        {
            var query = new InToEdgeConnectionQuery("Edge", "NodeIn", "NodeOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithCursor("test-cursor");

            Assert.IsInstanceOfType(result.Query, typeof(InToEdgeConnectionQuery));
            var resultQuery = (InToEdgeConnectionQuery)result.Query;
            Assert.AreEqual("test-cursor", resultQuery.Page.After);
        }

        [TestMethod]
        public void WithCursorInToAllEdgeConnectionQueryReturnsSameTypeWithUpdatedCursor()
        {
            var query = new InToAllEdgeConnectionQuery("NodeIn", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithCursor("test-cursor");

            Assert.IsInstanceOfType(result.Query, typeof(InToAllEdgeConnectionQuery));
            var resultQuery = (InToAllEdgeConnectionQuery)result.Query;
            Assert.AreEqual("test-cursor", resultQuery.Page.After);
        }

        [TestMethod]
        public void WithCursorOutToEdgeConnectionQueryReturnsSameTypeWithUpdatedCursor()
        {
            var query = new OutToEdgeConnectionQuery("Edge", "NodeIn", "NodeOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithCursor("test-cursor");

            Assert.IsInstanceOfType(result.Query, typeof(OutToEdgeConnectionQuery));
            var resultQuery = (OutToEdgeConnectionQuery)result.Query;
            Assert.AreEqual("test-cursor", resultQuery.Page.After);
        }

        [TestMethod]
        public void WithCursorOutToAllEdgeConnectionQueryReturnsSameTypeWithUpdatedCursor()
        {
            var query = new OutToAllEdgeConnectionQuery("NodeOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithCursor("test-cursor");

            Assert.IsInstanceOfType(result.Query, typeof(OutToAllEdgeConnectionQuery));
            var resultQuery = (OutToAllEdgeConnectionQuery)result.Query;
            Assert.AreEqual("test-cursor", resultQuery.Page.After);
        }

        [TestMethod]
        public void WithCursorInAndOutToEdgeConnectionQueryReturnsSameTypeWithUpdatedCursor()
        {
            var query = new InAndOutToEdgeConnectionQuery("Edge", "NodeInAndOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithCursor("test-cursor");

            Assert.IsInstanceOfType(result.Query, typeof(InAndOutToEdgeConnectionQuery));
            var resultQuery = (InAndOutToEdgeConnectionQuery)result.Query;
            Assert.AreEqual("test-cursor", resultQuery.Page.After);
        }

        [TestMethod]
        public void WithCursorZipNodeConnectionQueryReturnsSameTypeWithUpdatedCursor()
        {
            var query = new ZipNodeConnectionQuery(GraphlessDB.Collections.ImmutableTree<string, GraphQueryNode>.Empty, ConnectionArguments.Default, 25, null);
            var node = new GraphQueryNode(query);
            var result = node.WithCursor("test-cursor");

            Assert.IsInstanceOfType(result.Query, typeof(ZipNodeConnectionQuery));
            var resultQuery = (ZipNodeConnectionQuery)result.Query;
            Assert.AreEqual("test-cursor", resultQuery.Page.After);
        }

        [TestMethod]
        public void WithCursorWhereNodeConnectionQueryReturnsSameTypeWithUpdatedCursor()
        {
            var query = new WhereNodeConnectionQuery(_ => System.Threading.Tasks.Task.FromResult(true), ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithCursor("test-cursor");

            Assert.IsInstanceOfType(result.Query, typeof(WhereNodeConnectionQuery));
            var resultQuery = (WhereNodeConnectionQuery)result.Query;
            Assert.AreEqual("test-cursor", resultQuery.Page.After);
        }

        [TestMethod]
        public void WithCursorWhereEdgeConnectionQueryReturnsSameTypeWithUpdatedCursor()
        {
            var query = new WhereEdgeConnectionQuery(_ => System.Threading.Tasks.Task.FromResult(true), ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithCursor("test-cursor");

            Assert.IsInstanceOfType(result.Query, typeof(WhereEdgeConnectionQuery));
            var resultQuery = (WhereEdgeConnectionQuery)result.Query;
            Assert.AreEqual("test-cursor", resultQuery.Page.After);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void WithCursorUnsupportedQueryTypeThrowsNotSupportedException()
        {
            var query = new FirstNodeQuery(null);
            var node = new GraphQueryNode(query);
            node.WithCursor("test-cursor");
        }

        #endregion

        #region SupportsConnectionArguments Tests

        [TestMethod]
        public void SupportsConnectionArgumentsNodeConnectionQueryReturnsTrue()
        {
            var query = new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            Assert.IsTrue(node.SupportsConnectionArguments());
        }

        [TestMethod]
        public void SupportsConnectionArgumentsInToEdgeConnectionQueryReturnsTrue()
        {
            var query = new InToEdgeConnectionQuery("Edge", "NodeIn", "NodeOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            Assert.IsTrue(node.SupportsConnectionArguments());
        }

        [TestMethod]
        public void SupportsConnectionArgumentsInToAllEdgeConnectionQueryReturnsTrue()
        {
            var query = new InToAllEdgeConnectionQuery("NodeIn", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            Assert.IsTrue(node.SupportsConnectionArguments());
        }

        [TestMethod]
        public void SupportsConnectionArgumentsOutToEdgeConnectionQueryReturnsTrue()
        {
            var query = new OutToEdgeConnectionQuery("Edge", "NodeIn", "NodeOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            Assert.IsTrue(node.SupportsConnectionArguments());
        }

        [TestMethod]
        public void SupportsConnectionArgumentsOutToAllEdgeConnectionQueryReturnsTrue()
        {
            var query = new OutToAllEdgeConnectionQuery("NodeOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            Assert.IsTrue(node.SupportsConnectionArguments());
        }

        [TestMethod]
        public void SupportsConnectionArgumentsInAndOutToEdgeConnectionQueryReturnsTrue()
        {
            var query = new InAndOutToEdgeConnectionQuery("Edge", "NodeInAndOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            Assert.IsTrue(node.SupportsConnectionArguments());
        }

        [TestMethod]
        public void SupportsConnectionArgumentsZipNodeConnectionQueryReturnsTrue()
        {
            var query = new ZipNodeConnectionQuery(GraphlessDB.Collections.ImmutableTree<string, GraphQueryNode>.Empty, ConnectionArguments.Default, 25, null);
            var node = new GraphQueryNode(query);
            Assert.IsTrue(node.SupportsConnectionArguments());
        }

        [TestMethod]
        public void SupportsConnectionArgumentsWhereNodeConnectionQueryReturnsTrue()
        {
            var query = new WhereNodeConnectionQuery(_ => System.Threading.Tasks.Task.FromResult(true), ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            Assert.IsTrue(node.SupportsConnectionArguments());
        }

        [TestMethod]
        public void SupportsConnectionArgumentsWhereEdgeConnectionQueryReturnsTrue()
        {
            var query = new WhereEdgeConnectionQuery(_ => System.Threading.Tasks.Task.FromResult(true), ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            Assert.IsTrue(node.SupportsConnectionArguments());
        }

        [TestMethod]
        public void SupportsConnectionArgumentsUnsupportedQueryTypeReturnsFalse()
        {
            var query = new FirstNodeQuery(null);
            var node = new GraphQueryNode(query);
            Assert.IsFalse(node.SupportsConnectionArguments());
        }

        #endregion

        #region GetConnectionArguments Tests

        [TestMethod]
        public void GetConnectionArgumentsNodeConnectionQueryReturnsPageArguments()
        {
            var page = ConnectionArguments.GetFirst(10, "cursor1");
            var query = new NodeConnectionQuery("User", null, null, page, 25, false, null);
            var node = new GraphQueryNode(query);

            var result = node.GetConnectionArguments();
            Assert.AreEqual(page, result);
        }

        [TestMethod]
        public void GetConnectionArgumentsInToEdgeConnectionQueryReturnsPageArguments()
        {
            var page = ConnectionArguments.GetFirst(15, "cursor2");
            var query = new InToEdgeConnectionQuery("Edge", "NodeIn", "NodeOut", null, null, page, 25, false, null);
            var node = new GraphQueryNode(query);

            var result = node.GetConnectionArguments();
            Assert.AreEqual(page, result);
        }

        [TestMethod]
        public void GetConnectionArgumentsWhereNodeConnectionQueryReturnsPageArguments()
        {
            var page = ConnectionArguments.GetFirst(20);
            var query = new WhereNodeConnectionQuery(_ => System.Threading.Tasks.Task.FromResult(true), page, 25, false, null);
            var node = new GraphQueryNode(query);

            var result = node.GetConnectionArguments();
            Assert.AreEqual(page, result);
        }

        [TestMethod]
        public void GetConnectionArgumentsInToAllEdgeConnectionQueryReturnsPageArguments()
        {
            var page = ConnectionArguments.GetFirst(12);
            var query = new InToAllEdgeConnectionQuery("NodeIn", null, null, page, 25, false, null);
            var node = new GraphQueryNode(query);

            var result = node.GetConnectionArguments();
            Assert.AreEqual(page, result);
        }

        [TestMethod]
        public void GetConnectionArgumentsOutToAllEdgeConnectionQueryReturnsPageArguments()
        {
            var page = ConnectionArguments.GetFirst(18);
            var query = new OutToAllEdgeConnectionQuery("NodeOut", null, null, page, 25, false, null);
            var node = new GraphQueryNode(query);

            var result = node.GetConnectionArguments();
            Assert.AreEqual(page, result);
        }

        [TestMethod]
        public void GetConnectionArgumentsInAndOutToEdgeConnectionQueryReturnsPageArguments()
        {
            var page = ConnectionArguments.GetFirst(22);
            var query = new InAndOutToEdgeConnectionQuery("Edge", "NodeInAndOut", null, null, page, 25, false, null);
            var node = new GraphQueryNode(query);

            var result = node.GetConnectionArguments();
            Assert.AreEqual(page, result);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void GetConnectionArgumentsUnsupportedQueryTypeThrowsNotSupportedException()
        {
            var query = new FirstNodeQuery(null);
            var node = new GraphQueryNode(query);
            node.GetConnectionArguments();
        }

        #endregion

        #region WithConnectionSize Tests

        [TestMethod]
        public void WithConnectionSizeNodeConnectionQueryUpdatesPageSize()
        {
            var query = new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConnectionSize(50);

            Assert.IsInstanceOfType(result.Query, typeof(NodeConnectionQuery));
            var resultQuery = (NodeConnectionQuery)result.Query;
            Assert.AreEqual(50, resultQuery.Page.First);
        }

        [TestMethod]
        public void WithConnectionSizeInToEdgeConnectionQueryUpdatesPageSize()
        {
            var query = new InToEdgeConnectionQuery("Edge", "NodeIn", "NodeOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConnectionSize(100);

            Assert.IsInstanceOfType(result.Query, typeof(InToEdgeConnectionQuery));
            var resultQuery = (InToEdgeConnectionQuery)result.Query;
            Assert.AreEqual(100, resultQuery.Page.First);
        }

        [TestMethod]
        public void WithConnectionSizeWhereEdgeConnectionQueryUpdatesPageSize()
        {
            var query = new WhereEdgeConnectionQuery(_ => System.Threading.Tasks.Task.FromResult(true), ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConnectionSize(75);

            Assert.IsInstanceOfType(result.Query, typeof(WhereEdgeConnectionQuery));
            var resultQuery = (WhereEdgeConnectionQuery)result.Query;
            Assert.AreEqual(75, resultQuery.Page.First);
        }

        [TestMethod]
        public void WithConnectionSizeInToAllEdgeConnectionQueryUpdatesPageSize()
        {
            var query = new InToAllEdgeConnectionQuery("NodeIn", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConnectionSize(65);

            Assert.IsInstanceOfType(result.Query, typeof(InToAllEdgeConnectionQuery));
            var resultQuery = (InToAllEdgeConnectionQuery)result.Query;
            Assert.AreEqual(65, resultQuery.Page.First);
        }

        [TestMethod]
        public void WithConnectionSizeOutToAllEdgeConnectionQueryUpdatesPageSize()
        {
            var query = new OutToAllEdgeConnectionQuery("NodeOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConnectionSize(60);

            Assert.IsInstanceOfType(result.Query, typeof(OutToAllEdgeConnectionQuery));
            var resultQuery = (OutToAllEdgeConnectionQuery)result.Query;
            Assert.AreEqual(60, resultQuery.Page.First);
        }

        [TestMethod]
        public void WithConnectionSizeInAndOutToEdgeConnectionQueryUpdatesPageSize()
        {
            var query = new InAndOutToEdgeConnectionQuery("Edge", "NodeInAndOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConnectionSize(90);

            Assert.IsInstanceOfType(result.Query, typeof(InAndOutToEdgeConnectionQuery));
            var resultQuery = (InAndOutToEdgeConnectionQuery)result.Query;
            Assert.AreEqual(90, resultQuery.Page.First);
        }

        [TestMethod]
        public void WithConnectionSizeZipNodeConnectionQueryUpdatesPageSize()
        {
            var query = new ZipNodeConnectionQuery(GraphlessDB.Collections.ImmutableTree<string, GraphQueryNode>.Empty, ConnectionArguments.Default, 25, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConnectionSize(80);

            Assert.IsInstanceOfType(result.Query, typeof(ZipNodeConnectionQuery));
            var resultQuery = (ZipNodeConnectionQuery)result.Query;
            Assert.AreEqual(80, resultQuery.Page.First);
        }

        [TestMethod]
        public void WithConnectionSizeWhereNodeConnectionQueryUpdatesPageSize()
        {
            var query = new WhereNodeConnectionQuery(_ => System.Threading.Tasks.Task.FromResult(true), ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConnectionSize(55);

            Assert.IsInstanceOfType(result.Query, typeof(WhereNodeConnectionQuery));
            var resultQuery = (WhereNodeConnectionQuery)result.Query;
            Assert.AreEqual(55, resultQuery.Page.First);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void WithConnectionSizeUnsupportedQueryTypeThrowsNotSupportedException()
        {
            var query = new FirstNodeQuery(null);
            var node = new GraphQueryNode(query);
            node.WithConnectionSize(50);
        }

        #endregion

        #region WithPreFilteredConnectionSize Tests

        [TestMethod]
        public void WithPreFilteredConnectionSizeNodeConnectionQueryUpdatesPreFilteredPageSize()
        {
            var query = new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithPreFilteredConnectionSize(100);

            Assert.IsInstanceOfType(result.Query, typeof(NodeConnectionQuery));
            var resultQuery = (NodeConnectionQuery)result.Query;
            Assert.AreEqual(100, resultQuery.PreFilteredPageSize);
        }

        [TestMethod]
        public void WithPreFilteredConnectionSizeInToEdgeConnectionQueryUpdatesPreFilteredPageSize()
        {
            var query = new InToEdgeConnectionQuery("Edge", "NodeIn", "NodeOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithPreFilteredConnectionSize(200);

            Assert.IsInstanceOfType(result.Query, typeof(InToEdgeConnectionQuery));
            var resultQuery = (InToEdgeConnectionQuery)result.Query;
            Assert.AreEqual(200, resultQuery.PreFilteredPageSize);
        }

        [TestMethod]
        public void WithPreFilteredConnectionSizeZipNodeConnectionQueryUpdatesPreFilteredPageSize()
        {
            var query = new ZipNodeConnectionQuery(GraphlessDB.Collections.ImmutableTree<string, GraphQueryNode>.Empty, ConnectionArguments.Default, 25, null);
            var node = new GraphQueryNode(query);
            var result = node.WithPreFilteredConnectionSize(150);

            Assert.IsInstanceOfType(result.Query, typeof(ZipNodeConnectionQuery));
            var resultQuery = (ZipNodeConnectionQuery)result.Query;
            Assert.AreEqual(150, resultQuery.PreFilteredPageSize);
        }

        [TestMethod]
        public void WithPreFilteredConnectionSizeInToAllEdgeConnectionQueryUpdatesPreFilteredPageSize()
        {
            var query = new InToAllEdgeConnectionQuery("NodeIn", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithPreFilteredConnectionSize(175);

            Assert.IsInstanceOfType(result.Query, typeof(InToAllEdgeConnectionQuery));
            var resultQuery = (InToAllEdgeConnectionQuery)result.Query;
            Assert.AreEqual(175, resultQuery.PreFilteredPageSize);
        }

        [TestMethod]
        public void WithPreFilteredConnectionSizeOutToAllEdgeConnectionQueryUpdatesPreFilteredPageSize()
        {
            var query = new OutToAllEdgeConnectionQuery("NodeOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithPreFilteredConnectionSize(125);

            Assert.IsInstanceOfType(result.Query, typeof(OutToAllEdgeConnectionQuery));
            var resultQuery = (OutToAllEdgeConnectionQuery)result.Query;
            Assert.AreEqual(125, resultQuery.PreFilteredPageSize);
        }

        [TestMethod]
        public void WithPreFilteredConnectionSizeInAndOutToEdgeConnectionQueryUpdatesPreFilteredPageSize()
        {
            var query = new InAndOutToEdgeConnectionQuery("Edge", "NodeInAndOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithPreFilteredConnectionSize(165);

            Assert.IsInstanceOfType(result.Query, typeof(InAndOutToEdgeConnectionQuery));
            var resultQuery = (InAndOutToEdgeConnectionQuery)result.Query;
            Assert.AreEqual(165, resultQuery.PreFilteredPageSize);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void WithPreFilteredConnectionSizeUnsupportedQueryTypeThrowsNotSupportedException()
        {
            var query = new FirstNodeQuery(null);
            var node = new GraphQueryNode(query);
            node.WithPreFilteredConnectionSize(100);
        }

        #endregion

        #region WithConnectionArguments Tests

        [TestMethod]
        public void WithConnectionArgumentsNodeConnectionQueryUpdatesPageArguments()
        {
            var query = new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var newPage = ConnectionArguments.GetFirst(50, "new-cursor");
            var result = node.WithConnectionArguments(newPage);

            Assert.IsInstanceOfType(result.Query, typeof(NodeConnectionQuery));
            var resultQuery = (NodeConnectionQuery)result.Query;
            Assert.AreEqual(newPage, resultQuery.Page);
        }

        [TestMethod]
        public void WithConnectionArgumentsInToEdgeConnectionQueryUpdatesPageArguments()
        {
            var query = new InToEdgeConnectionQuery("Edge", "NodeIn", "NodeOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var newPage = ConnectionArguments.GetFirst(75, "edge-cursor");
            var result = node.WithConnectionArguments(newPage);

            Assert.IsInstanceOfType(result.Query, typeof(InToEdgeConnectionQuery));
            var resultQuery = (InToEdgeConnectionQuery)result.Query;
            Assert.AreEqual(newPage, resultQuery.Page);
        }

        [TestMethod]
        public void WithConnectionArgumentsWhereNodeConnectionQueryUpdatesPageArguments()
        {
            var query = new WhereNodeConnectionQuery(_ => System.Threading.Tasks.Task.FromResult(true), ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var newPage = ConnectionArguments.GetFirst(30);
            var result = node.WithConnectionArguments(newPage);

            Assert.IsInstanceOfType(result.Query, typeof(WhereNodeConnectionQuery));
            var resultQuery = (WhereNodeConnectionQuery)result.Query;
            Assert.AreEqual(newPage, resultQuery.Page);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void WithConnectionArgumentsUnsupportedQueryTypeThrowsNotSupportedException()
        {
            var query = new FirstNodeQuery(null);
            var node = new GraphQueryNode(query);
            var newPage = ConnectionArguments.GetFirst(50);
            node.WithConnectionArguments(newPage);
        }

        #endregion

        #region WithConsistentRead Tests

        [TestMethod]
        public void WithConsistentReadFirstNodeQueryReturnsSameInstance()
        {
            var query = new FirstNodeQuery(null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.AreSame(node, result);
        }

        [TestMethod]
        public void WithConsistentReadFirstOrDefaultNodeQueryReturnsSameInstance()
        {
            var query = new FirstOrDefaultNodeQuery(null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(false);

            Assert.AreSame(node, result);
        }

        [TestMethod]
        public void WithConsistentReadFromEdgeConnectionQueryUpdatesConsistentRead()
        {
            var query = new InFromEdgeConnectionQuery(null, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.IsInstanceOfType(result.Query, typeof(InFromEdgeConnectionQuery));
            var resultQuery = (InFromEdgeConnectionQuery)result.Query;
            Assert.IsTrue(resultQuery.ConsistentRead);
        }

        [TestMethod]
        public void WithConsistentReadFromEdgeQueryUpdatesConsistentRead()
        {
            var query = new InFromEdgeQuery(false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.IsInstanceOfType(result.Query, typeof(InFromEdgeQuery));
            var resultQuery = (InFromEdgeQuery)result.Query;
            Assert.IsTrue(resultQuery.ConsistentRead);
        }

        [TestMethod]
        public void WithConsistentReadToEdgeConnectionQueryUpdatesConsistentRead()
        {
            var query = new InToEdgeConnectionQuery("Edge", "NodeIn", "NodeOut", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.IsInstanceOfType(result.Query, typeof(InToEdgeConnectionQuery));
            var resultQuery = (InToEdgeConnectionQuery)result.Query;
            Assert.IsTrue(resultQuery.ConsistentRead);
        }

        [TestMethod]
        public void WithConsistentReadNodeByIdQueryUpdatesConsistentRead()
        {
            var query = new NodeByIdQuery("node-id", false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.IsInstanceOfType(result.Query, typeof(NodeByIdQuery));
            var resultQuery = (NodeByIdQuery)result.Query;
            Assert.IsTrue(resultQuery.ConsistentRead);
        }

        [TestMethod]
        public void WithConsistentReadNodeByNodeQueryUpdatesConsistentRead()
        {
            var user = User.New("testuser");
            var query = new NodeByNodeQuery(user, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.IsInstanceOfType(result.Query, typeof(NodeByNodeQuery));
            var resultQuery = (NodeByNodeQuery)result.Query;
            Assert.IsTrue(resultQuery.ConsistentRead);
        }

        [TestMethod]
        public void WithConsistentReadNodeOrDefaultByIdQueryUpdatesConsistentRead()
        {
            var query = new NodeOrDefaultByIdQuery("node-id", false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.IsInstanceOfType(result.Query, typeof(NodeOrDefaultByIdQuery));
            var resultQuery = (NodeOrDefaultByIdQuery)result.Query;
            Assert.IsTrue(resultQuery.ConsistentRead);
        }

        [TestMethod]
        public void WithConsistentReadNodeVersionByIdQueryUpdatesConsistentRead()
        {
            var query = new NodeVersionByIdQuery("node-id", 1, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.IsInstanceOfType(result.Query, typeof(NodeVersionByIdQuery));
            var resultQuery = (NodeVersionByIdQuery)result.Query;
            Assert.IsTrue(resultQuery.ConsistentRead);
        }

        [TestMethod]
        public void WithConsistentReadSingleEdgeQueryReturnsSameInstance()
        {
            var query = new SingleEdgeQuery(null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.AreSame(node, result);
        }

        [TestMethod]
        public void WithConsistentReadSingleNodeQueryReturnsSameInstance()
        {
            var query = new SingleNodeQuery(null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(false);

            Assert.AreSame(node, result);
        }

        [TestMethod]
        public void WithConsistentReadSingleOrDefaultEdgeQueryReturnsSameInstance()
        {
            var query = new SingleOrDefaultEdgeQuery(null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.AreSame(node, result);
        }

        [TestMethod]
        public void WithConsistentReadSingleOrDefaultNodeQueryReturnsSameInstance()
        {
            var query = new SingleOrDefaultNodeQuery(null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(false);

            Assert.AreSame(node, result);
        }

        [TestMethod]
        public void WithConsistentReadFirstEdgeQueryReturnsSameInstance()
        {
            var query = new FirstEdgeQuery(null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.AreSame(node, result);
        }

        [TestMethod]
        public void WithConsistentReadFirstOrDefaultEdgeQueryReturnsSameInstance()
        {
            var query = new FirstOrDefaultEdgeQuery(null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(false);

            Assert.AreSame(node, result);
        }

        [TestMethod]
        public void WithConsistentReadZipNodeConnectionQueryReturnsSameInstance()
        {
            var query = new ZipNodeConnectionQuery(GraphlessDB.Collections.ImmutableTree<string, GraphQueryNode>.Empty, ConnectionArguments.Default, 25, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.AreSame(node, result);
        }

        [TestMethod]
        public void WithConsistentReadWhereNodeConnectionQueryReturnsSameInstance()
        {
            var query = new WhereNodeConnectionQuery(_ => System.Threading.Tasks.Task.FromResult(true), ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.AreSame(node, result);
        }

        [TestMethod]
        public void WithConsistentReadWhereEdgeConnectionQueryReturnsSameInstance()
        {
            var query = new WhereEdgeConnectionQuery(_ => System.Threading.Tasks.Task.FromResult(true), ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(false);

            Assert.AreSame(node, result);
        }

        [TestMethod]
        public void WithConsistentReadEdgeByIdQueryUpdatesConsistentRead()
        {
            var query = new EdgeByIdQuery("Edge", "in-id", "out-id", false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.IsInstanceOfType(result.Query, typeof(EdgeByIdQuery));
            var resultQuery = (EdgeByIdQuery)result.Query;
            Assert.IsTrue(resultQuery.ConsistentRead);
        }

        [TestMethod]
        public void WithConsistentReadEdgeOrDefaultByIdQueryUpdatesConsistentRead()
        {
            var query = new EdgeOrDefaultByIdQuery("Edge", "in-id", "out-id", false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.IsInstanceOfType(result.Query, typeof(EdgeOrDefaultByIdQuery));
            var resultQuery = (EdgeOrDefaultByIdQuery)result.Query;
            Assert.IsTrue(resultQuery.ConsistentRead);
        }

        [TestMethod]
        public void WithConsistentReadNodeConnectionQueryUpdatesConsistentRead()
        {
            var query = new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, false, null);
            var node = new GraphQueryNode(query);
            var result = node.WithConsistentRead(true);

            Assert.IsInstanceOfType(result.Query, typeof(NodeConnectionQuery));
            var resultQuery = (NodeConnectionQuery)result.Query;
            Assert.IsTrue(resultQuery.ConsistentRead);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void WithConsistentReadUnsupportedQueryTypeThrowsNotSupportedException()
        {
            var query = new UnsupportedQuery();
            var node = new GraphQueryNode(query);
            node.WithConsistentRead(true);
        }

        #endregion

        private sealed record UnsupportedQuery() : GraphQuery;
    }
}
