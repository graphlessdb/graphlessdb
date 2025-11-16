/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Linq;
using GraphlessDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Graph.Tests
{
    [TestClass]
    public sealed class CursorTests
    {
        [TestMethod]
        public void HasTypeCursorsAreEquatable()
        {
            var cursor1 = CreateHasTypeCursor();
            var cursor2 = CreateHasTypeCursor();
            Assert.AreEqual(1, cursor1.GetNodeCount());
            Assert.AreEqual(1, cursor2.GetNodeCount());
            Assert.AreEqual(cursor1, cursor2);
        }

        [TestMethod]
        public void HasTypeHasInEdgeCursorHashCodesAreEqual()
        {
            var cursor1 = CreateHasTypeHasInEdgeCursor();
            var cursor2 = CreateHasTypeHasInEdgeCursor();
            Assert.AreEqual(cursor1.Items.Nodes.GetHashCode(), cursor2.Items.Nodes.GetHashCode());
            Assert.AreEqual(cursor1.Items.Nodes, cursor2.Items.Nodes);
            Assert.AreEqual(cursor1.Items.Edges.GetHashCode(), cursor2.Items.Edges.GetHashCode());
            Assert.AreEqual(cursor1.Items.Edges, cursor2.Items.Edges);
            Assert.AreEqual(cursor1.GetHashCode(), cursor2.GetHashCode());
            Assert.AreEqual(cursor1, cursor2);
        }

        [TestMethod]
        public void ExtractedSubTreeIsEqual()
        {
            var cursor1 = CreateHasTypeCursor();
            var cursor2 = CreateHasTypeHasInEdgeCursor();
            var cursor2SubTree = cursor2.GetSubTree(cursor2.GetChildNodeKeys(cursor2.GetRootKey()).Single());

            Assert.AreEqual(1, cursor1.GetNodeCount());
            Assert.AreEqual(2, cursor2.GetNodeCount());
            Assert.AreEqual(1, cursor2SubTree.GetNodeCount());
            Assert.AreEqual(cursor1, cursor2SubTree);
        }

        [TestMethod]
        public void ExtractedTripleLayelSubTreeIsEqual()
        {
            var cursor1 = CreateHasTypeHasInEdgeCursor();
            var cursor2 = CreateHasTypeHasInEdgeHasInEdgeCursor();
            var cursor2SubTree = cursor2.GetSubTree(cursor2.GetChildNodeKeys(cursor2.GetRootKey()).Single());

            Assert.AreEqual(2, cursor1.GetNodeCount());
            Assert.AreEqual(3, cursor2.GetNodeCount());
            Assert.AreEqual(2, cursor2SubTree.GetNodeCount());
            Assert.AreEqual(cursor1, cursor2SubTree);
        }

        [TestMethod]
        public void ExtractedQuadLayelSubTreeIsEqual()
        {
            var cursor1 = CreateHasTypeHasInEdgeHasInEdgeCursor();
            var cursor2 = CreateHasTypeHasInEdgeHasInEdgeHasInEdgeCursor();
            var cursor2SubTree = cursor2.GetSubTree(cursor2.GetChildNodeKeys(cursor2.GetRootKey()).Single());

            Assert.AreEqual(3, cursor1.GetNodeCount());
            Assert.AreEqual(4, cursor2.GetNodeCount());
            Assert.AreEqual(3, cursor2SubTree.GetNodeCount());
            Assert.AreEqual(cursor1, cursor2SubTree);
        }

        private static Cursor CreateHasTypeCursor()
        {
            return Cursor.Create(CursorNode.Empty with { HasType = new HasTypeCursor("Subject", "Partition", []) });
        }

        private static Cursor CreateHasTypeHasInEdgeCursor()
        {
            return Cursor
                .Create(CursorNode.Empty with { HasType = new HasTypeCursor("Subject", "Partition", []) })
                .AddAsParentToRoot(CursorNode.Empty with { HasInEdge = new HasInEdgeCursor("Subject", "EdgeTypeName", "NodeOutId") });
        }

        private static Cursor CreateHasTypeHasInEdgeHasInEdgeCursor()
        {
            return Cursor
                .Create(CursorNode.Empty with { HasType = new HasTypeCursor("Subject", "Partition", []) })
                .AddAsParentToRoot(CursorNode.Empty with { HasInEdge = new HasInEdgeCursor("Subject", "EdgeTypeName", "NodeOutId") })
                .AddAsParentToRoot(CursorNode.Empty with { HasInEdge = new HasInEdgeCursor("Subject2", "EdgeTypeName", "NodeOutId") });
        }

        private static Cursor CreateHasTypeHasInEdgeHasInEdgeHasInEdgeCursor()
        {
            return Cursor
                .Create(CursorNode.Empty with { HasType = new HasTypeCursor("Subject", "Partition", []) })
                .AddAsParentToRoot(CursorNode.Empty with { HasInEdge = new HasInEdgeCursor("Subject", "EdgeTypeName", "NodeOutId") })
                .AddAsParentToRoot(CursorNode.Empty with { HasInEdge = new HasInEdgeCursor("Subject2", "EdgeTypeName", "NodeOutId") })
                .AddAsParentToRoot(CursorNode.Empty with { HasInEdge = new HasInEdgeCursor("Subject3", "EdgeTypeName", "NodeOutId") });
        }
    }
}
