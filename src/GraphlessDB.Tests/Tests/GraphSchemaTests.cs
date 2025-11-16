/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using GraphlessDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test method names are more readable with underscores")]
    public sealed class GraphSchemaTests
    {
        #region Constructor and Properties Tests

        [TestMethod]
        public void ConstructorSetsNodesProperty()
        {
            var nodes = ImmutableList.Create("Node1", "Node2");
            var nodesByType = ImmutableHashSet<string>.Empty;
            var edges = ImmutableList<EdgeSchema>.Empty;
            var edgesByType = ImmutableDictionary<string, EdgeSchema>.Empty;

            var graphSchema = new GraphSchema(nodes, nodesByType, edges, edgesByType);

            Assert.AreEqual(nodes, graphSchema.Nodes);
        }

        [TestMethod]
        public void ConstructorSetsNodesByTypeProperty()
        {
            var nodes = ImmutableList<string>.Empty;
            var nodesByType = ImmutableHashSet.Create("Type1", "Type2");
            var edges = ImmutableList<EdgeSchema>.Empty;
            var edgesByType = ImmutableDictionary<string, EdgeSchema>.Empty;

            var graphSchema = new GraphSchema(nodes, nodesByType, edges, edgesByType);

            Assert.AreEqual(nodesByType, graphSchema.NodesByType);
        }

        [TestMethod]
        public void ConstructorSetsEdgesProperty()
        {
            var nodes = ImmutableList<string>.Empty;
            var nodesByType = ImmutableHashSet<string>.Empty;
            var edge1 = new EdgeSchema("Edge1", "InType", EdgeCardinality.One, "OutType", EdgeCardinality.ZeroOrMany);
            var edges = ImmutableList.Create(edge1);
            var edgesByType = ImmutableDictionary<string, EdgeSchema>.Empty;

            var graphSchema = new GraphSchema(nodes, nodesByType, edges, edgesByType);

            Assert.AreEqual(edges, graphSchema.Edges);
        }

        [TestMethod]
        public void ConstructorSetsEdgesByTypeProperty()
        {
            var nodes = ImmutableList<string>.Empty;
            var nodesByType = ImmutableHashSet<string>.Empty;
            var edges = ImmutableList<EdgeSchema>.Empty;
            var edge1 = new EdgeSchema("Edge1", "InType", EdgeCardinality.One, "OutType", EdgeCardinality.ZeroOrMany);
            var edgesByType = ImmutableDictionary.Create<string, EdgeSchema>().Add("Edge1", edge1);

            var graphSchema = new GraphSchema(nodes, nodesByType, edges, edgesByType);

            Assert.AreEqual(edgesByType, graphSchema.EdgesByType);
        }

        #endregion

        #region Equality Tests

        [TestMethod]
        public void EqualsReturnsTrueWhenAllPropertiesMatch()
        {
            var nodes = ImmutableList.Create("Node1");
            var nodesByType = ImmutableHashSet.Create("Type1");
            var edge1 = new EdgeSchema("Edge1", "InType", EdgeCardinality.One, "OutType", EdgeCardinality.ZeroOrMany);
            var edges = ImmutableList.Create(edge1);
            var edgesByType = ImmutableDictionary.Create<string, EdgeSchema>().Add("Edge1", edge1);

            var graphSchema1 = new GraphSchema(nodes, nodesByType, edges, edgesByType);
            var graphSchema2 = new GraphSchema(nodes, nodesByType, edges, edgesByType);

            Assert.IsTrue(graphSchema1.Equals(graphSchema2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenNodesDiffer()
        {
            var nodes1 = ImmutableList.Create("Node1");
            var nodes2 = ImmutableList.Create("Node2");
            var nodesByType = ImmutableHashSet.Create("Type1");
            var edges = ImmutableList<EdgeSchema>.Empty;
            var edgesByType = ImmutableDictionary<string, EdgeSchema>.Empty;

            var graphSchema1 = new GraphSchema(nodes1, nodesByType, edges, edgesByType);
            var graphSchema2 = new GraphSchema(nodes2, nodesByType, edges, edgesByType);

            Assert.IsFalse(graphSchema1.Equals(graphSchema2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenNodesByTypeDiffer()
        {
            var nodes = ImmutableList.Create("Node1");
            var nodesByType1 = ImmutableHashSet.Create("Type1");
            var nodesByType2 = ImmutableHashSet.Create("Type2");
            var edges = ImmutableList<EdgeSchema>.Empty;
            var edgesByType = ImmutableDictionary<string, EdgeSchema>.Empty;

            var graphSchema1 = new GraphSchema(nodes, nodesByType1, edges, edgesByType);
            var graphSchema2 = new GraphSchema(nodes, nodesByType2, edges, edgesByType);

            Assert.IsFalse(graphSchema1.Equals(graphSchema2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenEdgesDiffer()
        {
            var nodes = ImmutableList.Create("Node1");
            var nodesByType = ImmutableHashSet.Create("Type1");
            var edge1 = new EdgeSchema("Edge1", "InType", EdgeCardinality.One, "OutType", EdgeCardinality.ZeroOrMany);
            var edge2 = new EdgeSchema("Edge2", "InType2", EdgeCardinality.ZeroOrMany, "OutType2", EdgeCardinality.One);
            var edges1 = ImmutableList.Create(edge1);
            var edges2 = ImmutableList.Create(edge2);
            var edgesByType = ImmutableDictionary<string, EdgeSchema>.Empty;

            var graphSchema1 = new GraphSchema(nodes, nodesByType, edges1, edgesByType);
            var graphSchema2 = new GraphSchema(nodes, nodesByType, edges2, edgesByType);

            Assert.IsFalse(graphSchema1.Equals(graphSchema2));
        }

        [TestMethod]
        public void EqualsReturnsFalseWhenEdgesByTypeDiffer()
        {
            var nodes = ImmutableList.Create("Node1");
            var nodesByType = ImmutableHashSet.Create("Type1");
            var edges = ImmutableList<EdgeSchema>.Empty;
            var edge1 = new EdgeSchema("Edge1", "InType", EdgeCardinality.One, "OutType", EdgeCardinality.ZeroOrMany);
            var edge2 = new EdgeSchema("Edge2", "InType2", EdgeCardinality.ZeroOrMany, "OutType2", EdgeCardinality.One);
            var edgesByType1 = ImmutableDictionary.Create<string, EdgeSchema>().Add("Edge1", edge1);
            var edgesByType2 = ImmutableDictionary.Create<string, EdgeSchema>().Add("Edge2", edge2);

            var graphSchema1 = new GraphSchema(nodes, nodesByType, edges, edgesByType1);
            var graphSchema2 = new GraphSchema(nodes, nodesByType, edges, edgesByType2);

            Assert.IsFalse(graphSchema1.Equals(graphSchema2));
        }

        #endregion

        #region GetHashCode Tests

        [TestMethod]
        public void GetHashCodeReturnsSameValueForEqualObjects()
        {
            var nodes = ImmutableList.Create("Node1");
            var nodesByType = ImmutableHashSet.Create("Type1");
            var edge1 = new EdgeSchema("Edge1", "InType", EdgeCardinality.One, "OutType", EdgeCardinality.ZeroOrMany);
            var edges = ImmutableList.Create(edge1);
            var edgesByType = ImmutableDictionary.Create<string, EdgeSchema>().Add("Edge1", edge1);

            var graphSchema1 = new GraphSchema(nodes, nodesByType, edges, edgesByType);
            var graphSchema2 = new GraphSchema(nodes, nodesByType, edges, edgesByType);

            Assert.AreEqual(graphSchema1.GetHashCode(), graphSchema2.GetHashCode());
        }

        #endregion
    }
}
