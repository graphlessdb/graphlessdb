/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using GraphlessDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class NodesNotFoundExceptionTests
    {
        #region Constructor Tests

        [TestMethod]
        public void DefaultConstructorCreatesExceptionWithEmptyNodeIds()
        {
            var exception = new NodesNotFoundException();
            Assert.IsNotNull(exception.NodeIds);
            Assert.AreEqual(0, exception.NodeIds.Count);
        }

        [TestMethod]
        public void ConstructorWithSingleNodeIdSetsNodeIdsProperty()
        {
            var nodeId = "node123";
            var exception = new NodesNotFoundException(nodeId);
            Assert.AreEqual(1, exception.NodeIds.Count);
            Assert.AreEqual(nodeId, exception.NodeIds[0]);
        }

        [TestMethod]
        public void ConstructorWithNodeIdsListSetsNodeIdsProperty()
        {
            var nodeIds = ImmutableList.Create("node1", "node2", "node3");
            var exception = new NodesNotFoundException(nodeIds);
            Assert.AreEqual(nodeIds, exception.NodeIds);
            Assert.AreEqual(3, exception.NodeIds.Count);
        }

        [TestMethod]
        public void ConstructorWithEmptyListSetsEmptyNodeIds()
        {
            var nodeIds = ImmutableList<string>.Empty;
            var exception = new NodesNotFoundException(nodeIds);
            Assert.IsNotNull(exception.NodeIds);
            Assert.AreEqual(0, exception.NodeIds.Count);
        }

        [TestMethod]
        public void ConstructorWithSingleNodeIdSetsCorrectMessage()
        {
            var nodeId = "node123";
            var exception = new NodesNotFoundException(nodeId);
            Assert.IsTrue(exception.Message.Contains(nodeId));
        }

        [TestMethod]
        public void ConstructorWithMultipleNodeIdsSetsCorrectMessage()
        {
            var nodeIds = ImmutableList.Create("node1", "node2");
            var exception = new NodesNotFoundException(nodeIds);
            Assert.IsTrue(exception.Message.Contains("node1"));
            Assert.IsTrue(exception.Message.Contains("node2"));
        }

        [TestMethod]
        public void DefaultConstructorSetsCorrectMessage()
        {
            var exception = new NodesNotFoundException();
            Assert.IsNotNull(exception.Message);
            Assert.IsTrue(exception.Message.Contains("Nodes") || exception.Message.Contains("could not be found"));
        }

        #endregion

        #region NodeIds Property Tests

        [TestMethod]
        public void NodeIdsPropertyReturnsCorrectList()
        {
            var nodeId1 = "node1";
            var nodeId2 = "node2";
            var nodeId3 = "node3";
            var nodeIds = ImmutableList.Create(nodeId1, nodeId2, nodeId3);
            var exception = new NodesNotFoundException(nodeIds);
            Assert.AreEqual(3, exception.NodeIds.Count);
            Assert.AreEqual(nodeId1, exception.NodeIds[0]);
            Assert.AreEqual(nodeId2, exception.NodeIds[1]);
            Assert.AreEqual(nodeId3, exception.NodeIds[2]);
        }

        #endregion
    }
}
