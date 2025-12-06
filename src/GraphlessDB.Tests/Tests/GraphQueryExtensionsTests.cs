/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using GraphlessDB.Collections;
using GraphlessDB.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class GraphQueryExtensionsTests
    {
        private static ImmutableTree<string, GraphQueryNode> CreateTreeWithNodes(params (string key, GraphQueryNode node)[] nodes)
        {
            var tree = ImmutableTree<string, GraphQueryNode>.Empty;
            foreach (var (key, node) in nodes)
            {
                tree = tree.AddNode(key, node);
            }
            return tree;
        }

        private static GraphQueryNode CreateNodeConnectionQueryNode(bool consistentRead = false, string? after = null)
        {
            var page = after != null ? ConnectionArguments.GetFirst(25, after) : ConnectionArguments.Default;
            var query = new NodeConnectionQuery("User", null, null, page, 25, consistentRead, null);
            return new GraphQueryNode(query);
        }

        private static GraphQueryNode CreateNodeByIdQueryNode(bool consistentRead = false)
        {
            var query = new NodeByIdQuery("id1", consistentRead, null);
            return new GraphQueryNode(query);
        }

        #region WithConsistentRead Tests

        [TestMethod]
        public void WithConsistentReadEmptyTreeReturnsEmptyTree()
        {
            var tree = ImmutableTree<string, GraphQueryNode>.Empty;
            var result = tree.WithConsistentRead(true);

            Assert.AreEqual(0, result.Nodes.ByKey.Count);
        }

        [TestMethod]
        public void WithConsistentReadSingleNodeUpdatesConsistentRead()
        {
            var node = CreateNodeConnectionQueryNode(false);
            var tree = CreateTreeWithNodes(("key1", node));

            var result = tree.WithConsistentRead(true);

            Assert.AreEqual(1, result.Nodes.ByKey.Count);
            var resultNode = result.GetNode("key1");
            var resultQuery = (NodeConnectionQuery)resultNode.Query;
            Assert.IsTrue(resultQuery.ConsistentRead);
        }

        [TestMethod]
        public void WithConsistentReadMultipleNodesUpdatesAllNodes()
        {
            var node1 = CreateNodeConnectionQueryNode(false);
            var node2 = CreateNodeConnectionQueryNode(false);
            var tree = CreateTreeWithNodes(("key1", node1), ("key2", node2));

            var result = tree.WithConsistentRead(true);

            Assert.AreEqual(2, result.Nodes.ByKey.Count);
            var resultNode1 = result.GetNode("key1");
            var resultQuery1 = (NodeConnectionQuery)resultNode1.Query;
            Assert.IsTrue(resultQuery1.ConsistentRead);

            var resultNode2 = result.GetNode("key2");
            var resultQuery2 = (NodeConnectionQuery)resultNode2.Query;
            Assert.IsTrue(resultQuery2.ConsistentRead);
        }

        [TestMethod]
        public void WithConsistentReadSetToFalseUpdatesConsistentRead()
        {
            var node = CreateNodeConnectionQueryNode(true);
            var tree = CreateTreeWithNodes(("key1", node));

            var result = tree.WithConsistentRead(false);

            var resultNode = result.GetNode("key1");
            var resultQuery = (NodeConnectionQuery)resultNode.Query;
            Assert.IsFalse(resultQuery.ConsistentRead);
        }

        #endregion

        #region TryGetRootConnectionArgumentsKey Tests

        [TestMethod]
        public void TryGetRootConnectionArgumentsKeyRootNodeSupportsConnectionArgumentsReturnsTrue()
        {
            var node = CreateNodeConnectionQueryNode();
            var tree = CreateTreeWithNodes(("root", node));

            var result = tree.TryGetRootConnectionArgumentsKey(out var resultKey);

            Assert.IsTrue(result);
            Assert.AreEqual("root", resultKey);
        }

        [TestMethod]
        public void TryGetRootConnectionArgumentsKeyRootNodeDoesNotSupportConnectionArgumentsReturnsFalse()
        {
            var node = CreateNodeByIdQueryNode();
            var tree = CreateTreeWithNodes(("root", node));

            var result = tree.TryGetRootConnectionArgumentsKey(out var resultKey);

            Assert.IsFalse(result);
            Assert.AreEqual("root", resultKey);
        }

        [TestMethod]
        public void TryGetRootConnectionArgumentsKeyMultipleNodesReturnsRootKey()
        {
            var rootNode = CreateNodeConnectionQueryNode();
            var childNode = CreateNodeConnectionQueryNode();
            var tree = CreateTreeWithNodes(("root", rootNode), ("child", childNode));
            tree = tree.AddEdge("child", "root");

            var result = tree.TryGetRootConnectionArgumentsKey(out var resultKey);

            Assert.IsTrue(result);
            Assert.AreEqual("root", resultKey);
        }

        #endregion

        #region IsRootConnectionArgumentsKey Tests

        [TestMethod]
        public void IsRootConnectionArgumentsKeyMatchingRootKeyReturnsTrue()
        {
            var node = CreateNodeConnectionQueryNode();
            var tree = CreateTreeWithNodes(("root", node));

            var result = tree.IsRootConnectionArgumentsKey("root");

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsRootConnectionArgumentsKeyNonMatchingKeyReturnsFalse()
        {
            var node = CreateNodeConnectionQueryNode();
            var tree = CreateTreeWithNodes(("root", node));

            var result = tree.IsRootConnectionArgumentsKey("other");

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsRootConnectionArgumentsKeyChildKeyReturnsFalse()
        {
            var rootNode = CreateNodeConnectionQueryNode();
            var childNode = CreateNodeConnectionQueryNode();
            var tree = CreateTreeWithNodes(("root", rootNode), ("child", childNode));
            tree = tree.AddEdge("child", "root");

            var result = tree.IsRootConnectionArgumentsKey("child");

            Assert.IsFalse(result);
        }

        #endregion

        #region WithIntermediateConnectionSize Tests

        [TestMethod]
        public void WithIntermediateConnectionSizeEmptyTreeReturnsEmptyTree()
        {
            var tree = ImmutableTree<string, GraphQueryNode>.Empty;
            var result = tree.WithIntermediateConnectionSize(50);

            Assert.AreEqual(0, result.Nodes.ByKey.Count);
        }

        [TestMethod]
        public void WithIntermediateConnectionSizeRootNodeNotUpdated()
        {
            var node = CreateNodeConnectionQueryNode();
            var tree = CreateTreeWithNodes(("root", node));

            var result = tree.WithIntermediateConnectionSize(50);

            var resultNode = result.GetNode("root");
            var resultQuery = (NodeConnectionQuery)resultNode.Query;
            Assert.AreEqual(25, resultQuery.Page.Count());
        }

        [TestMethod]
        public void WithIntermediateConnectionSizeIntermediateNodeUpdated()
        {
            var rootNode = CreateNodeConnectionQueryNode();
            var intermediateNode = CreateNodeConnectionQueryNode();
            var tree = CreateTreeWithNodes(("root", rootNode), ("intermediate", intermediateNode));
            tree = tree.AddEdge("intermediate", "root");

            var result = tree.WithIntermediateConnectionSize(50);

            var resultNode = result.GetNode("intermediate");
            var resultQuery = (NodeConnectionQuery)resultNode.Query;
            Assert.AreEqual(50, resultQuery.Page.Count());
        }

        [TestMethod]
        public void WithIntermediateConnectionSizeMultipleIntermediateNodesUpdated()
        {
            var rootNode = CreateNodeConnectionQueryNode();
            var intermediate1 = CreateNodeConnectionQueryNode();
            var intermediate2 = CreateNodeConnectionQueryNode();
            var tree = CreateTreeWithNodes(("root", rootNode), ("int1", intermediate1), ("int2", intermediate2));
            tree = tree.AddEdge("int1", "root");
            tree = tree.AddEdge("int2", "int1");

            var result = tree.WithIntermediateConnectionSize(75);

            var resultNode1 = result.GetNode("int1");
            var resultQuery1 = (NodeConnectionQuery)resultNode1.Query;
            Assert.AreEqual(75, resultQuery1.Page.Count());

            var resultNode2 = result.GetNode("int2");
            var resultQuery2 = (NodeConnectionQuery)resultNode2.Query;
            Assert.AreEqual(75, resultQuery2.Page.Count());
        }

        [TestMethod]
        public void WithIntermediateConnectionSizeNodeNotSupportingConnectionArgumentsNotUpdated()
        {
            var rootNode = CreateNodeConnectionQueryNode();
            var nonConnectionNode = CreateNodeByIdQueryNode();
            var tree = CreateTreeWithNodes(("root", rootNode), ("other", nonConnectionNode));
            tree = tree.AddEdge("other", "root");

            var result = tree.WithIntermediateConnectionSize(50);

            var resultNode = result.GetNode("other");
            Assert.IsInstanceOfType(resultNode.Query, typeof(NodeByIdQuery));
        }

        #endregion

        #region WithPreFilteredConnectionSize Tests

        [TestMethod]
        public void WithPreFilteredConnectionSizeEmptyTreeReturnsEmptyTree()
        {
            var tree = ImmutableTree<string, GraphQueryNode>.Empty;
            var result = tree.WithPreFilteredConnectionSize(100);

            Assert.AreEqual(0, result.Nodes.ByKey.Count);
        }

        [TestMethod]
        public void WithPreFilteredConnectionSizeSingleNodeUpdatesPreFilteredPageSize()
        {
            var node = CreateNodeConnectionQueryNode();
            var tree = CreateTreeWithNodes(("key1", node));

            var result = tree.WithPreFilteredConnectionSize(100);

            var resultNode = result.GetNode("key1");
            var resultQuery = (NodeConnectionQuery)resultNode.Query;
            Assert.AreEqual(100, resultQuery.PreFilteredPageSize);
        }

        [TestMethod]
        public void WithPreFilteredConnectionSizeMultipleNodesUpdatesAllNodes()
        {
            var node1 = CreateNodeConnectionQueryNode();
            var node2 = CreateNodeConnectionQueryNode();
            var tree = CreateTreeWithNodes(("key1", node1), ("key2", node2));

            var result = tree.WithPreFilteredConnectionSize(150);

            var resultNode1 = result.GetNode("key1");
            var resultQuery1 = (NodeConnectionQuery)resultNode1.Query;
            Assert.AreEqual(150, resultQuery1.PreFilteredPageSize);

            var resultNode2 = result.GetNode("key2");
            var resultQuery2 = (NodeConnectionQuery)resultNode2.Query;
            Assert.AreEqual(150, resultQuery2.PreFilteredPageSize);
        }

        [TestMethod]
        public void WithPreFilteredConnectionSizeNodeNotSupportingConnectionArgumentsNotUpdated()
        {
            var connectionNode = CreateNodeConnectionQueryNode();
            var nonConnectionNode = CreateNodeByIdQueryNode();
            var tree = CreateTreeWithNodes(("conn", connectionNode), ("other", nonConnectionNode));

            var result = tree.WithPreFilteredConnectionSize(100);

            var connectionResult = result.GetNode("conn");
            var connectionQuery = (NodeConnectionQuery)connectionResult.Query;
            Assert.AreEqual(100, connectionQuery.PreFilteredPageSize);

            var otherResult = result.GetNode("other");
            Assert.IsInstanceOfType(otherResult.Query, typeof(NodeByIdQuery));
        }

        #endregion

        #region WithConnectionArguments Tests

        [TestMethod]
        public void WithConnectionArgumentsNoRootConnectionKeyReturnsOriginalTree()
        {
            var node = CreateNodeByIdQueryNode();
            var tree = CreateTreeWithNodes(("key1", node));
            var newArgs = ConnectionArguments.GetFirst(50, "cursor1");

            var result = tree.WithConnectionArguments(newArgs);

            var resultNode = result.GetNode("key1");
            Assert.IsInstanceOfType(resultNode.Query, typeof(NodeByIdQuery));
        }

        [TestMethod]
        public void WithConnectionArgumentsRootNodeUpdatesConnectionArguments()
        {
            var node = CreateNodeConnectionQueryNode();
            var tree = CreateTreeWithNodes(("root", node));
            var newArgs = ConnectionArguments.GetFirst(50, "cursor1");

            var result = tree.WithConnectionArguments(newArgs);

            var resultNode = result.GetNode("root");
            var resultQuery = (NodeConnectionQuery)resultNode.Query;
            Assert.AreEqual(50, resultQuery.Page.Count());
            Assert.AreEqual("cursor1", resultQuery.Page.After);
        }

        [TestMethod]
        public void WithConnectionArgumentsOnlyRootNodeUpdated()
        {
            var rootNode = CreateNodeConnectionQueryNode();
            var childNode = CreateNodeConnectionQueryNode();
            var tree = CreateTreeWithNodes(("root", rootNode), ("child", childNode));
            tree = tree.AddEdge("child", "root");
            var newArgs = ConnectionArguments.GetFirst(50, "cursor1");

            var result = tree.WithConnectionArguments(newArgs);

            var resultRootNode = result.GetNode("root");
            var resultRootQuery = (NodeConnectionQuery)resultRootNode.Query;
            Assert.AreEqual(50, resultRootQuery.Page.Count());
            Assert.AreEqual("cursor1", resultRootQuery.Page.After);

            var resultChildNode = result.GetNode("child");
            var resultChildQuery = (NodeConnectionQuery)resultChildNode.Query;
            Assert.AreEqual(25, resultChildQuery.Page.Count());
            Assert.IsNull(resultChildQuery.Page.After);
        }

        #endregion

        #region HasCursor Tests

        [TestMethod]
        public void HasCursorEmptyTreeReturnsFalse()
        {
            var tree = ImmutableTree<string, GraphQueryNode>.Empty;
            var result = tree.HasCursor();

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void HasCursorNodeWithoutCursorReturnsFalse()
        {
            var node = CreateNodeConnectionQueryNode(false, null);
            var tree = CreateTreeWithNodes(("key1", node));

            var result = tree.HasCursor();

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void HasCursorNodeWithCursorReturnsTrue()
        {
            var node = CreateNodeConnectionQueryNode(false, "cursor1");
            var tree = CreateTreeWithNodes(("key1", node));

            var result = tree.HasCursor();

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasCursorMultipleNodesWithOneCursorReturnsTrue()
        {
            var nodeWithCursor = CreateNodeConnectionQueryNode(false, "cursor1");
            var nodeWithoutCursor = CreateNodeConnectionQueryNode(false, null);
            var tree = CreateTreeWithNodes(("key1", nodeWithCursor), ("key2", nodeWithoutCursor));

            var result = tree.HasCursor();

            Assert.IsTrue(result);
        }

        [TestMethod]
        public void HasCursorNodeNotSupportingConnectionArgumentsReturnsFalse()
        {
            var node = CreateNodeByIdQueryNode();
            var tree = CreateTreeWithNodes(("key1", node));

            var result = tree.HasCursor();

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void HasCursorMixedNodeTypesWithCursorReturnsTrue()
        {
            var nodeWithCursor = CreateNodeConnectionQueryNode(false, "cursor1");
            var nodeWithoutConnection = CreateNodeByIdQueryNode();
            var tree = CreateTreeWithNodes(("key1", nodeWithCursor), ("key2", nodeWithoutConnection));

            var result = tree.HasCursor();

            Assert.IsTrue(result);
        }

        #endregion
    }
}
