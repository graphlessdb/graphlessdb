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
using GraphlessDB.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class ImmutableTreeExtensionsTests
    {
        private static ImmutableTree<string, string> CreateEmptyTree()
        {
            return ImmutableTree<string, string>.Empty;
        }

        private static ImmutableTree<string, string> CreateTreeWithNodes(params (string key, string value)[] nodes)
        {
            var tree = CreateEmptyTree();
            foreach (var (key, value) in nodes)
            {
                tree = tree.AddNode(key, value);
            }
            return tree;
        }

        private static ImmutableTree<string, string> CreateSimpleTreeWithRoot()
        {
            return CreateTreeWithNodes(("root", "Root"));
        }

        private static ImmutableTree<string, string> CreateTreeWithRootAndChildren()
        {
            var tree = CreateTreeWithNodes(("root", "Root"), ("child1", "Child1"), ("child2", "Child2"));
            tree = tree.AddEdge("child1", "root");
            tree = tree.AddEdge("child2", "root");
            return tree;
        }

        #region GetRootKeyOrDefault Tests

        [TestMethod]
        public void GetRootKeyOrDefaultReturnsRootKeyWhenExists()
        {
            var tree = CreateSimpleTreeWithRoot();

            var rootKey = tree.GetRootKeyOrDefault();

            Assert.AreEqual("root", rootKey);
        }

        [TestMethod]
        public void GetRootKeyOrDefaultReturnsDefaultWhenTreeIsEmpty()
        {
            var tree = CreateEmptyTree();

            var rootKey = tree.GetRootKeyOrDefault();

            Assert.AreEqual(default, rootKey);
        }

        [TestMethod]
        public void GetRootKeyOrDefaultReturnsRootKeyWhenTreeHasChildren()
        {
            var tree = CreateTreeWithRootAndChildren();

            var rootKey = tree.GetRootKeyOrDefault();

            Assert.AreEqual("root", rootKey);
        }

        #endregion

        #region GetRootKey Tests

        [TestMethod]
        public void GetRootKeyReturnsRootKeyWhenExists()
        {
            var tree = CreateSimpleTreeWithRoot();

            var rootKey = tree.GetRootKey();

            Assert.AreEqual("root", rootKey);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetRootKeyThrowsWhenTreeIsEmpty()
        {
            var tree = CreateEmptyTree();

            tree.GetRootKey();
        }

        [TestMethod]
        public void GetRootKeyReturnsRootKeyWhenTreeHasChildren()
        {
            var tree = CreateTreeWithRootAndChildren();

            var rootKey = tree.GetRootKey();

            Assert.AreEqual("root", rootKey);
        }

        #endregion

        #region GetRootNodeOrDefault Tests

        [TestMethod]
        public void GetRootNodeOrDefaultReturnsRootNodeWhenExists()
        {
            var tree = CreateSimpleTreeWithRoot();

            var rootNode = tree.GetRootNodeOrDefault();

            Assert.AreEqual("Root", rootNode);
        }

        [TestMethod]
        public void GetRootNodeOrDefaultReturnsDefaultWhenTreeIsEmpty()
        {
            var tree = CreateEmptyTree();

            var rootNode = tree.GetRootNodeOrDefault();

            Assert.AreEqual(default, rootNode);
        }

        [TestMethod]
        public void GetRootNodeOrDefaultReturnsRootNodeWhenTreeHasChildren()
        {
            var tree = CreateTreeWithRootAndChildren();

            var rootNode = tree.GetRootNodeOrDefault();

            Assert.AreEqual("Root", rootNode);
        }

        #endregion

        #region GetRootNode Tests

        [TestMethod]
        public void GetRootNodeReturnsRootNodeWhenExists()
        {
            var tree = CreateSimpleTreeWithRoot();

            var rootNode = tree.GetRootNode();

            Assert.AreEqual("Root", rootNode);
        }

        [TestMethod]
        public void GetRootNodeReturnsRootNodeWhenTreeHasChildren()
        {
            var tree = CreateTreeWithRootAndChildren();

            var rootNode = tree.GetRootNode();

            Assert.AreEqual("Root", rootNode);
        }

        #endregion

        #region GetNode Tests

        [TestMethod]
        public void GetNodeReturnsNodeWhenKeyExists()
        {
            var tree = CreateTreeWithNodes(("node1", "Node1"), ("node2", "Node2"));

            var node = tree.GetNode("node1");

            Assert.AreEqual("Node1", node);
        }

        [TestMethod]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void GetNodeThrowsWhenKeyDoesNotExist()
        {
            var tree = CreateSimpleTreeWithRoot();

            tree.GetNode("nonexistent");
        }

        #endregion

        #region TryGetNode Tests

        [TestMethod]
        public void TryGetNodeReturnsNodeWhenKeyExists()
        {
            var tree = CreateTreeWithNodes(("node1", "Node1"), ("node2", "Node2"));

            var node = tree.TryGetNode("node1");

            Assert.AreEqual("Node1", node);
        }

        [TestMethod]
        public void TryGetNodeReturnsDefaultWhenKeyDoesNotExist()
        {
            var tree = CreateSimpleTreeWithRoot();

            var node = tree.TryGetNode("nonexistent");

            Assert.AreEqual(default, node);
        }

        #endregion

        #region GetSingleChildNodeKey Tests

        [TestMethod]
        public void GetSingleChildNodeKeyReturnsChildKeyWhenExists()
        {
            var tree = CreateTreeWithNodes(("root", "Root"), ("child", "Child"));
            tree = tree.AddEdge("child", "root");

            var childKey = tree.GetSingleChildNodeKey("root");

            Assert.AreEqual("child", childKey);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetSingleChildNodeKeyThrowsWhenNodeHasNoChildren()
        {
            var tree = CreateSimpleTreeWithRoot();

            tree.GetSingleChildNodeKey("root");
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetSingleChildNodeKeyThrowsWhenNodeHasMultipleChildren()
        {
            var tree = CreateTreeWithRootAndChildren();

            tree.GetSingleChildNodeKey("root");
        }

        #endregion

        #region GetSingleOrDefaultChildNodeKey Tests

        [TestMethod]
        public void GetSingleOrDefaultChildNodeKeyReturnsChildKeyWhenExists()
        {
            var tree = CreateTreeWithNodes(("root", "Root"), ("child", "Child"));
            tree = tree.AddEdge("child", "root");

            var childKey = tree.GetSingleOrDefaultChildNodeKey("root");

            Assert.AreEqual("child", childKey);
        }

        [TestMethod]
        public void GetSingleOrDefaultChildNodeKeyReturnsDefaultWhenNodeHasNoChildren()
        {
            var tree = CreateSimpleTreeWithRoot();

            var childKey = tree.GetSingleOrDefaultChildNodeKey("root");

            Assert.AreEqual(default, childKey);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetSingleOrDefaultChildNodeKeyThrowsWhenNodeHasMultipleChildren()
        {
            var tree = CreateTreeWithRootAndChildren();

            tree.GetSingleOrDefaultChildNodeKey("root");
        }

        #endregion

        #region GetSingleChildNode Tests

        [TestMethod]
        public void GetSingleChildNodeReturnsChildNodeWhenExists()
        {
            var tree = CreateTreeWithNodes(("root", "Root"), ("child", "Child"));
            tree = tree.AddEdge("child", "root");

            var childNode = tree.GetSingleChildNode("root");

            Assert.AreEqual("Child", childNode);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetSingleChildNodeThrowsWhenNodeHasNoChildren()
        {
            var tree = CreateSimpleTreeWithRoot();

            tree.GetSingleChildNode("root");
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetSingleChildNodeThrowsWhenNodeHasMultipleChildren()
        {
            var tree = CreateTreeWithRootAndChildren();

            tree.GetSingleChildNode("root");
        }

        #endregion

        #region TryGetParentNode Tests

        [TestMethod]
        public void TryGetParentNodeReturnsParentNodeWhenExists()
        {
            var tree = CreateTreeWithNodes(("root", "Root"), ("child", "Child"));
            tree = tree.AddEdge("child", "root");

            var parentNode = tree.TryGetParentNode("child");

            Assert.AreEqual("Root", parentNode);
        }

        [TestMethod]
        public void TryGetParentNodeReturnsDefaultWhenNodeHasNoParent()
        {
            var tree = CreateSimpleTreeWithRoot();

            var parentNode = tree.TryGetParentNode("root");

            Assert.AreEqual(default, parentNode);
        }

        #endregion

        #region GetChildNodes Tests

        [TestMethod]
        public void GetChildNodesReturnsChildrenWhenExist()
        {
            var tree = CreateTreeWithRootAndChildren();

            var children = tree.GetChildNodes("root");

            Assert.AreEqual(2, children.Count);
            Assert.IsTrue(children.Contains("Child1"));
            Assert.IsTrue(children.Contains("Child2"));
        }

        [TestMethod]
        public void GetChildNodesReturnsEmptyListWhenNodeHasNoChildren()
        {
            var tree = CreateSimpleTreeWithRoot();

            var children = tree.GetChildNodes("root");

            Assert.AreEqual(0, children.Count);
        }

        #endregion

        #region GetChildNodeKeys Tests

        [TestMethod]
        public void GetChildNodeKeysReturnsChildKeysWhenExist()
        {
            var tree = CreateTreeWithRootAndChildren();

            var childKeys = tree.GetChildNodeKeys("root");

            Assert.AreEqual(2, childKeys.Count);
            Assert.IsTrue(childKeys.Contains("child1"));
            Assert.IsTrue(childKeys.Contains("child2"));
        }

        [TestMethod]
        public void GetChildNodeKeysReturnsEmptyListWhenNodeHasNoChildren()
        {
            var tree = CreateSimpleTreeWithRoot();

            var childKeys = tree.GetChildNodeKeys("root");

            Assert.AreEqual(0, childKeys.Count);
        }

        #endregion

        #region GetParentKey Tests

        [TestMethod]
        public void GetParentKeyReturnsParentKeyWhenExists()
        {
            var tree = CreateTreeWithNodes(("root", "Root"), ("child", "Child"));
            tree = tree.AddEdge("child", "root");

            var parentKey = tree.GetParentKey("child");

            Assert.AreEqual("root", parentKey);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetParentKeyThrowsWhenNodeHasNoParent()
        {
            var tree = CreateSimpleTreeWithRoot();

            tree.GetParentKey("root");
        }

        #endregion

        #region TryGetParentKey Tests

        [TestMethod]
        public void TryGetParentKeyReturnsTrueAndParentKeyWhenExists()
        {
            var tree = CreateTreeWithNodes(("root", "Root"), ("child", "Child"));
            tree = tree.AddEdge("child", "root");

            var result = tree.TryGetParentKey("child", out var parentKey);

            Assert.IsTrue(result);
            Assert.AreEqual("root", parentKey);
        }

        [TestMethod]
        public void TryGetParentKeyReturnsFalseWhenNodeHasNoParent()
        {
            var tree = CreateSimpleTreeWithRoot();

            var result = tree.TryGetParentKey("root", out var parentKey);

            Assert.IsFalse(result);
            Assert.AreEqual(default, parentKey);
        }

        #endregion

        #region SetNode Tests

        [TestMethod]
        public void SetNodeUpdatesExistingNode()
        {
            var tree = CreateTreeWithNodes(("node1", "OldValue"));

            tree = tree.SetNode("node1", "NewValue");

            Assert.AreEqual("NewValue", tree.GetNode("node1"));
        }

        [TestMethod]
        public void SetNodeAddsNewNode()
        {
            var tree = CreateEmptyTree();

            tree = tree.SetNode("node1", "NewValue");

            Assert.AreEqual("NewValue", tree.GetNode("node1"));
        }

        #endregion

        #region AddNode Tests

        [TestMethod]
        public void AddNodeAddsNewNode()
        {
            var tree = CreateEmptyTree();

            tree = tree.AddNode("node1", "Node1");

            Assert.AreEqual("Node1", tree.GetNode("node1"));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void AddNodeThrowsWhenKeyAlreadyExists()
        {
            var tree = CreateTreeWithNodes(("node1", "Node1"));

            tree.AddNode("node1", "DuplicateNode");
        }

        #endregion

        #region AddParentNode Tests

        [TestMethod]
        public void AddParentNodeAddsNodeAndConnectsToChild()
        {
            var tree = CreateTreeWithNodes(("child", "Child"));

            tree = tree.AddParentNode("child", "parent", "Parent");

            Assert.AreEqual("Parent", tree.GetNode("parent"));
            Assert.AreEqual("parent", tree.GetParentKey("child"));
        }

        #endregion

        #region AddChildNode Tests

        [TestMethod]
        public void AddChildNodeAddsNodeAndConnectsToParent()
        {
            var tree = CreateTreeWithNodes(("parent", "Parent"));

            tree = tree.AddChildNode("parent", "child", "Child");

            Assert.AreEqual("Child", tree.GetNode("child"));
            Assert.AreEqual("parent", tree.GetParentKey("child"));
        }

        #endregion

        #region GetSubTree Tests

        [TestMethod]
        public void GetSubTreeReturnsSubTreeWithSingleNode()
        {
            var tree = CreateSimpleTreeWithRoot();

            var subTree = tree.GetSubTree("root");

            Assert.AreEqual(1, subTree.Nodes.ByKey.Count);
            Assert.AreEqual("Root", subTree.GetNode("root"));
        }

        [TestMethod]
        public void GetSubTreeReturnsSubTreeWithChildren()
        {
            var tree = CreateTreeWithRootAndChildren();

            var subTree = tree.GetSubTree("root");

            Assert.AreEqual(3, subTree.Nodes.ByKey.Count);
            Assert.AreEqual("Root", subTree.GetNode("root"));
            Assert.AreEqual("Child1", subTree.GetNode("child1"));
            Assert.AreEqual("Child2", subTree.GetNode("child2"));
        }

        [TestMethod]
        public void GetSubTreeReturnsSubTreeFromChildNode()
        {
            var tree = CreateTreeWithNodes(("root", "Root"), ("child", "Child"), ("grandchild", "GrandChild"));
            tree = tree.AddEdge("child", "root");
            tree = tree.AddEdge("grandchild", "child");

            var subTree = tree.GetSubTree("child");

            Assert.AreEqual(2, subTree.Nodes.ByKey.Count);
            Assert.AreEqual("Child", subTree.GetNode("child"));
            Assert.AreEqual("GrandChild", subTree.GetNode("grandchild"));
            Assert.IsFalse(subTree.Nodes.ByKey.ContainsKey("root"));
        }

        [TestMethod]
        public void GetSubTreePreservesTreeStructure()
        {
            var tree = CreateTreeWithNodes(("root", "Root"), ("child1", "Child1"), ("child2", "Child2"), ("grandchild", "GrandChild"));
            tree = tree.AddEdge("child1", "root");
            tree = tree.AddEdge("child2", "root");
            tree = tree.AddEdge("grandchild", "child1");

            var subTree = tree.GetSubTree("root");

            Assert.AreEqual(4, subTree.Nodes.ByKey.Count);
            var childKeys = subTree.GetChildNodeKeys("root");
            Assert.AreEqual(2, childKeys.Count);
            Assert.IsTrue(childKeys.Contains("child1"));
            Assert.IsTrue(childKeys.Contains("child2"));

            var grandChildKeys = subTree.GetChildNodeKeys("child1");
            Assert.AreEqual(1, grandChildKeys.Count);
            Assert.AreEqual("grandchild", grandChildKeys[0]);
        }

        #endregion

        #region AddSubTree Tests

        [TestMethod]
        public void AddSubTreeAddsAllNodesFromChildTree()
        {
            var tree = CreateTreeWithNodes(("root", "Root"));
            var childTree = CreateTreeWithNodes(("childroot", "ChildRoot"), ("childnode", "ChildNode"));
            childTree = childTree.AddEdge("childnode", "childroot");

            tree = tree.AddSubTree("root", childTree);

            Assert.AreEqual(3, tree.Nodes.ByKey.Count);
            Assert.AreEqual("Root", tree.GetNode("root"));
            Assert.AreEqual("ChildRoot", tree.GetNode("childroot"));
            Assert.AreEqual("ChildNode", tree.GetNode("childnode"));
        }

        [TestMethod]
        public void AddSubTreeConnectsChildTreeToParentNode()
        {
            var tree = CreateTreeWithNodes(("root", "Root"));
            var childTree = CreateTreeWithNodes(("childroot", "ChildRoot"));

            tree = tree.AddSubTree("root", childTree);

            var childKeys = tree.GetChildNodeKeys("root");
            Assert.AreEqual(1, childKeys.Count);
            Assert.AreEqual("childroot", childKeys[0]);
        }

        [TestMethod]
        public void AddSubTreePreservesChildTreeStructure()
        {
            var tree = CreateTreeWithNodes(("root", "Root"));
            var childTree = CreateTreeWithNodes(("childroot", "ChildRoot"), ("childnode1", "ChildNode1"), ("childnode2", "ChildNode2"));
            childTree = childTree.AddEdge("childnode1", "childroot");
            childTree = childTree.AddEdge("childnode2", "childroot");

            tree = tree.AddSubTree("root", childTree);

            var childKeys = tree.GetChildNodeKeys("childroot");
            Assert.AreEqual(2, childKeys.Count);
            Assert.IsTrue(childKeys.Contains("childnode1"));
            Assert.IsTrue(childKeys.Contains("childnode2"));
        }

        #endregion

        #region AddEdge Tests

        [TestMethod]
        public void AddEdgeConnectsTwoNodes()
        {
            var tree = CreateTreeWithNodes(("parent", "Parent"), ("child", "Child"));

            tree = tree.AddEdge("child", "parent");

            Assert.AreEqual("parent", tree.GetParentKey("child"));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void AddEdgeThrowsWhenParentNodeDoesNotExist()
        {
            var tree = CreateTreeWithNodes(("child", "Child"));

            tree.AddEdge("child", "parent");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void AddEdgeThrowsWhenChildNodeDoesNotExist()
        {
            var tree = CreateTreeWithNodes(("parent", "Parent"));

            tree.AddEdge("child", "parent");
        }

        [TestMethod]
        public void AddEdgeAllowsMultipleChildrenForSameParent()
        {
            var tree = CreateTreeWithNodes(("parent", "Parent"), ("child1", "Child1"), ("child2", "Child2"));
            tree = tree.AddEdge("child1", "parent");

            tree = tree.AddEdge("child2", "parent");

            var childKeys = tree.GetChildNodeKeys("parent");
            Assert.AreEqual(2, childKeys.Count);
            Assert.IsTrue(childKeys.Contains("child1"));
            Assert.IsTrue(childKeys.Contains("child2"));
        }

        [TestMethod]
        public void AddEdgeAllowsMultipleParentsForSameChild()
        {
            var tree = CreateTreeWithNodes(("parent1", "Parent1"), ("parent2", "Parent2"), ("child", "Child"));
            tree = tree.AddEdge("child", "parent1");

            tree = tree.AddEdge("child", "parent2");

            // Although this creates an invalid tree structure, the code allows it
            // We verify that the edge was added
            Assert.IsTrue(tree.Edges.ByOutKey.ContainsKey("child"));
            Assert.AreEqual(2, tree.Edges.ByOutKey["child"].Count);
        }

        #endregion

        #region TryFind Tests

        [TestMethod]
        public void TryFindReturnsTrueWhenPredicateMatchesRootKey()
        {
            var tree = CreateSimpleTreeWithRoot();

            var result = tree.TryFind(k => k == "root", "root", out var foundKey);

            Assert.IsTrue(result);
            Assert.AreEqual("root", foundKey);
        }

        [TestMethod]
        public void TryFindReturnsFalseWhenPredicateDoesNotMatch()
        {
            var tree = CreateSimpleTreeWithRoot();

            var result = tree.TryFind(k => k == "nonexistent", "root", out var foundKey);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public void TryFindSearchesChildNodesWhenRootDoesNotMatch()
        {
            var tree = CreateTreeWithRootAndChildren();

            var result = tree.TryFind(k => k == "child1", "root", out var foundKey);

            Assert.IsTrue(result);
            Assert.AreEqual("child1", foundKey);
        }

        [TestMethod]
        public void TryFindSearchesDeepChildNodesWhenIntermediateNodesDoNotMatch()
        {
            var tree = CreateTreeWithNodes(("root", "Root"), ("child", "Child"), ("grandchild", "GrandChild"));
            tree = tree.AddEdge("child", "root");
            tree = tree.AddEdge("grandchild", "child");

            var result = tree.TryFind(k => k == "grandchild", "root", out var foundKey);

            Assert.IsTrue(result);
            Assert.AreEqual("grandchild", foundKey);
        }

        [TestMethod]
        public void TryFindReturnsFalseWhenNoNodeMatches()
        {
            var tree = CreateTreeWithRootAndChildren();

            var result = tree.TryFind(k => k == "nonexistent", "root", out var foundKey);

            Assert.IsFalse(result);
        }

        #endregion
    }
}
