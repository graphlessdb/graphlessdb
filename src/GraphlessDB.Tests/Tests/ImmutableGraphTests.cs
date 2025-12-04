/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using GraphlessDB.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class ImmutableGraphTests
    {
        [TestMethod]
        public void CreateCreatesGraphWithEmptyNodesAndEdges()
        {
            var nodes = ImmutableList<string>.Empty;
            var edges = ImmutableList<string>.Empty;

            var result = ImmutableGraph.Create(
                nodes,
                n => n.GetHashCode(),
                edges,
                e => e.GetHashCode(),
                e => e.GetHashCode());

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Nodes);
            Assert.IsNotNull(result.Edges);
            Assert.AreEqual(0, result.Nodes.ByKey.Count);
        }

        [TestMethod]
        public void CreateCreatesGraphWithNodes()
        {
            var nodes = ImmutableList.Create("Node1", "Node2", "Node3");
            var edges = ImmutableList<string>.Empty;

            var result = ImmutableGraph.Create(
                nodes,
                n => n.GetHashCode(),
                edges,
                e => e.GetHashCode(),
                e => e.GetHashCode());

            Assert.IsNotNull(result);
            Assert.AreEqual(3, result.Nodes.ByKey.Count);
            Assert.IsTrue(result.Nodes.ByKey.ContainsKey("Node1".GetHashCode()));
            Assert.IsTrue(result.Nodes.ByKey.ContainsKey("Node2".GetHashCode()));
            Assert.IsTrue(result.Nodes.ByKey.ContainsKey("Node3".GetHashCode()));
        }

        [TestMethod]
        public void CreateCreatesGraphWithEdges()
        {
            var nodes = ImmutableList.Create("A", "B");
            var edges = ImmutableList.Create("EdgeAB");

            var result = ImmutableGraph.Create(
                nodes,
                n => n[0],
                edges,
                e => e[4],
                e => e[5]);

            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Nodes.ByKey.Count);
            Assert.IsTrue(result.Edges.ByInKey.ContainsKey('A'));
            Assert.IsTrue(result.Edges.ByOutKey.ContainsKey('B'));
        }

        [TestMethod]
        public void CreateCreatesGraphWithNodesAndMultipleEdges()
        {
            var nodes = ImmutableList.Create(1, 2, 3);
            var edges = ImmutableList.Create((from: 1, to: 2), (from: 2, to: 3), (from: 1, to: 3));

            var result = ImmutableGraph.Create(
                nodes,
                n => n,
                edges,
                e => e.from,
                e => e.to);

            Assert.IsNotNull(result);
            Assert.AreEqual(3, result.Nodes.ByKey.Count);
            Assert.AreEqual(2, result.Edges.ByInKey.Count);
            Assert.AreEqual(2, result.Edges.ByInKey[1].Count);
            Assert.AreEqual(1, result.Edges.ByInKey[2].Count);
        }

        [TestMethod]
        public void CreateCreatesGraphWithComplexTypes()
        {
            var nodes = ImmutableList.Create(
                new { Id = 1, Name = "Node1" },
                new { Id = 2, Name = "Node2" });
            var edges = ImmutableList.Create(
                new { From = 1, To = 2, Label = "Edge" });

            var result = ImmutableGraph.Create(
                nodes,
                n => n.Id,
                edges,
                e => e.From,
                e => e.To);

            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Nodes.ByKey.Count);
            Assert.IsTrue(result.Nodes.ByKey.ContainsKey(1));
            Assert.IsTrue(result.Nodes.ByKey.ContainsKey(2));
            Assert.AreEqual(1, result.Edges.ByInKey.Count);
            Assert.AreEqual(1, result.Edges.ByOutKey.Count);
        }
    }
}
