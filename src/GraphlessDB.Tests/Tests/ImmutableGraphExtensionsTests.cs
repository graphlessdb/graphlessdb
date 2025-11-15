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
using GraphlessDB.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class ImmutableGraphExtensionsTests
    {
        private static ImmutableGraph<string, string, int> CreateEmptyGraph()
        {
            return ImmutableGraph<string, string, int>.Empty;
        }

        private static ImmutableGraph<string, string, int> CreateGraphWithNodes(params (int key, string value)[] nodes)
        {
            var graph = CreateEmptyGraph();
            foreach (var (key, value) in nodes)
            {
                graph = graph.AddNode(key, value);
            }
            return graph;
        }

        #region GetRootKeys Tests

        [TestMethod]
        public void GetRootKeysReturnsAllKeysWhenNoEdges()
        {
            var graph = CreateGraphWithNodes((1, "Node1"), (2, "Node2"), (3, "Node3"));

            var rootKeys = graph.GetRootKeys();

            Assert.AreEqual(3, rootKeys.Count);
            Assert.IsTrue(rootKeys.Contains(1));
            Assert.IsTrue(rootKeys.Contains(2));
            Assert.IsTrue(rootKeys.Contains(3));
        }

        [TestMethod]
        public void GetRootKeysReturnsEmptyListWhenGraphIsEmpty()
        {
            var graph = CreateEmptyGraph();

            var rootKeys = graph.GetRootKeys();

            Assert.AreEqual(0, rootKeys.Count);
        }

        [TestMethod]
        public void GetRootKeysExcludesNodesWithIncomingEdges()
        {
            var graph = CreateGraphWithNodes((1, "Node1"), (2, "Node2"), (3, "Node3"));
            graph = graph.AddEdge(2, 1, "Edge1to2");

            var rootKeys = graph.GetRootKeys();

            Assert.AreEqual(2, rootKeys.Count);
            Assert.IsTrue(rootKeys.Contains(1));
            Assert.IsTrue(rootKeys.Contains(3));
        }

        [TestMethod]
        public void GetRootKeysHandlesMultipleEdgesToSameNode()
        {
            var graph = CreateGraphWithNodes((1, "Node1"), (2, "Node2"), (3, "Node3"));
            graph = graph.AddEdge(2, 1, "Edge1to2");
            graph = graph.AddEdge(2, 3, "Edge3to2");

            var rootKeys = graph.GetRootKeys();

            Assert.AreEqual(2, rootKeys.Count);
            Assert.IsTrue(rootKeys.Contains(1));
            Assert.IsTrue(rootKeys.Contains(3));
        }

        [TestMethod]
        public void GetRootKeysReturnsOnlyRootNodeInLinearChain()
        {
            var graph = CreateGraphWithNodes((1, "Node1"), (2, "Node2"), (3, "Node3"));
            graph = graph.AddEdge(2, 1, "Edge1to2");
            graph = graph.AddEdge(3, 2, "Edge2to3");

            var rootKeys = graph.GetRootKeys();

            Assert.AreEqual(1, rootKeys.Count);
            Assert.AreEqual(1, rootKeys[0]);
        }

        #endregion

        #region GetRootNodes Tests

        [TestMethod]
        public void GetRootNodesReturnsAllNodesWhenNoEdges()
        {
            var graph = CreateGraphWithNodes((1, "Node1"), (2, "Node2"), (3, "Node3"));

            var rootNodes = graph.GetRootNodes();

            Assert.AreEqual(3, rootNodes.Count);
            Assert.IsTrue(rootNodes.Contains("Node1"));
            Assert.IsTrue(rootNodes.Contains("Node2"));
            Assert.IsTrue(rootNodes.Contains("Node3"));
        }

        [TestMethod]
        public void GetRootNodesReturnsEmptyListWhenGraphIsEmpty()
        {
            var graph = CreateEmptyGraph();

            var rootNodes = graph.GetRootNodes();

            Assert.AreEqual(0, rootNodes.Count);
        }

        [TestMethod]
        public void GetRootNodesExcludesNodesWithIncomingEdges()
        {
            var graph = CreateGraphWithNodes((1, "Node1"), (2, "Node2"), (3, "Node3"));
            graph = graph.AddEdge(2, 1, "Edge1to2");

            var rootNodes = graph.GetRootNodes();

            Assert.AreEqual(2, rootNodes.Count);
            Assert.IsTrue(rootNodes.Contains("Node1"));
            Assert.IsTrue(rootNodes.Contains("Node3"));
        }

        [TestMethod]
        public void GetRootNodesReturnsCorrectNodesForComplexGraph()
        {
            var graph = CreateGraphWithNodes((1, "Node1"), (2, "Node2"), (3, "Node3"), (4, "Node4"));
            graph = graph.AddEdge(2, 1, "Edge1to2");
            graph = graph.AddEdge(3, 1, "Edge1to3");
            graph = graph.AddEdge(4, 2, "Edge2to4");

            var rootNodes = graph.GetRootNodes();

            Assert.AreEqual(1, rootNodes.Count);
            Assert.AreEqual("Node1", rootNodes[0]);
        }

        #endregion

        #region AddNode Tests

        [TestMethod]
        public void AddNodeAddsNodeToEmptyGraph()
        {
            var graph = CreateEmptyGraph();

            var result = graph.AddNode(1, "Node1");

            Assert.AreEqual(1, result.Nodes.ByKey.Count);
            Assert.IsTrue(result.Nodes.ByKey.ContainsKey(1));
            Assert.AreEqual("Node1", result.Nodes.ByKey[1]);
        }

        [TestMethod]
        public void AddNodeAddsNodeToExistingGraph()
        {
            var graph = CreateGraphWithNodes((1, "Node1"));

            var result = graph.AddNode(2, "Node2");

            Assert.AreEqual(2, result.Nodes.ByKey.Count);
            Assert.IsTrue(result.Nodes.ByKey.ContainsKey(1));
            Assert.IsTrue(result.Nodes.ByKey.ContainsKey(2));
            Assert.AreEqual("Node1", result.Nodes.ByKey[1]);
            Assert.AreEqual("Node2", result.Nodes.ByKey[2]);
        }

        [TestMethod]
        public void AddNodeDoesNotModifyOriginalGraph()
        {
            var graph = CreateGraphWithNodes((1, "Node1"));

            var result = graph.AddNode(2, "Node2");

            Assert.AreEqual(1, graph.Nodes.ByKey.Count);
            Assert.AreEqual(2, result.Nodes.ByKey.Count);
        }

        [TestMethod]
        public void AddNodeWithDuplicateKeyThrowsException()
        {
            var graph = CreateGraphWithNodes((1, "Node1"));

            Assert.ThrowsException<ArgumentException>(() => graph.AddNode(1, "Node1Updated"));
        }

        #endregion

        #region AddEdge Tests

        [TestMethod]
        public void AddEdgeAddsEdgeToGraphWithNodes()
        {
            var graph = CreateGraphWithNodes((1, "Node1"), (2, "Node2"));

            var result = graph.AddEdge(2, 1, "Edge1to2");

            Assert.IsTrue(result.Edges.ByInKey.ContainsKey(2));
            Assert.IsTrue(result.Edges.ByOutKey.ContainsKey(1));
            Assert.AreEqual(1, result.Edges.ByInKey[2].Count);
            Assert.AreEqual("Edge1to2", result.Edges.ByInKey[2][0]);
            Assert.AreEqual(1, result.Edges.ByOutKey[1].Count);
            Assert.AreEqual("Edge1to2", result.Edges.ByOutKey[1][0]);
        }

        [TestMethod]
        public void AddEdgeDoesNotModifyOriginalGraph()
        {
            var graph = CreateGraphWithNodes((1, "Node1"), (2, "Node2"));

            var result = graph.AddEdge(2, 1, "Edge1to2");

            Assert.AreEqual(0, graph.Edges.ByInKey.Count);
            Assert.AreEqual(0, graph.Edges.ByOutKey.Count);
            Assert.AreEqual(1, result.Edges.ByInKey.Count);
            Assert.AreEqual(1, result.Edges.ByOutKey.Count);
        }

        [TestMethod]
        public void AddEdgeThrowsExceptionWhenInKeyNotFound()
        {
            var graph = CreateGraphWithNodes((1, "Node1"));

            Assert.ThrowsException<ArgumentException>(() => graph.AddEdge(2, 1, "Edge1to2"));
        }

        [TestMethod]
        public void AddEdgeThrowsExceptionWhenOutKeyNotFound()
        {
            var graph = CreateGraphWithNodes((1, "Node1"));

            Assert.ThrowsException<ArgumentException>(() => graph.AddEdge(1, 2, "Edge2to1"));
        }

        [TestMethod]
        public void AddEdgeThrowsExceptionWhenBothKeysNotFound()
        {
            var graph = CreateEmptyGraph();

            Assert.ThrowsException<ArgumentException>(() => graph.AddEdge(1, 2, "Edge1to2"));
        }

        [TestMethod]
        public void AddEdgeAllowsMultipleEdgesFromSameInKey()
        {
            var graph = CreateGraphWithNodes((1, "Node1"), (2, "Node2"), (3, "Node3"));
            graph = graph.AddEdge(2, 1, "Edge1to2");

            var result = graph.AddEdge(3, 1, "Edge1to3");

            Assert.AreEqual(2, result.Edges.ByInKey.Count);
            Assert.IsTrue(result.Edges.ByInKey.ContainsKey(2));
            Assert.IsTrue(result.Edges.ByInKey.ContainsKey(3));
            Assert.AreEqual(2, result.Edges.ByOutKey[1].Count);
        }

        [TestMethod]
        public void AddEdgeAllowsMultipleEdgesToSameOutKey()
        {
            var graph = CreateGraphWithNodes((1, "Node1"), (2, "Node2"), (3, "Node3"));
            graph = graph.AddEdge(2, 1, "Edge1to2");

            var result = graph.AddEdge(2, 3, "Edge3to2");

            Assert.AreEqual(1, result.Edges.ByInKey.Count);
            Assert.AreEqual(2, result.Edges.ByInKey[2].Count);
            Assert.AreEqual(2, result.Edges.ByOutKey.Count);
            Assert.IsTrue(result.Edges.ByOutKey.ContainsKey(1));
            Assert.IsTrue(result.Edges.ByOutKey.ContainsKey(3));
        }

        [TestMethod]
        public void AddEdgeAllowsSelfLoop()
        {
            var graph = CreateGraphWithNodes((1, "Node1"));

            var result = graph.AddEdge(1, 1, "SelfLoop");

            Assert.IsTrue(result.Edges.ByInKey.ContainsKey(1));
            Assert.IsTrue(result.Edges.ByOutKey.ContainsKey(1));
            Assert.AreEqual(1, result.Edges.ByInKey[1].Count);
            Assert.AreEqual(1, result.Edges.ByOutKey[1].Count);
            Assert.AreEqual("SelfLoop", result.Edges.ByInKey[1][0]);
        }

        [TestMethod]
        public void AddEdgeHandlesComplexGraphStructure()
        {
            var graph = CreateGraphWithNodes((1, "Node1"), (2, "Node2"), (3, "Node3"), (4, "Node4"));
            graph = graph.AddEdge(2, 1, "Edge1to2");
            graph = graph.AddEdge(3, 1, "Edge1to3");
            graph = graph.AddEdge(3, 2, "Edge2to3");

            var result = graph.AddEdge(4, 3, "Edge3to4");

            Assert.AreEqual(3, result.Edges.ByInKey.Count);
            Assert.AreEqual(3, result.Edges.ByOutKey.Count);
            Assert.AreEqual(2, result.Edges.ByOutKey[1].Count);
            Assert.AreEqual(1, result.Edges.ByOutKey[2].Count);
            Assert.AreEqual(1, result.Edges.ByOutKey[3].Count);
        }

        #endregion
    }
}
