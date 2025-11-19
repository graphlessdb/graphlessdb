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
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using GraphlessDB;
using GraphlessDB.DynamoDB;
using GraphlessDB.Graph.Services;
using GraphlessDB.Storage;
using GraphlessDB.Storage.Services.DynamoDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Storage.Services.DynamoDB.Tests
{
    [TestClass]
    public sealed class AmazonDynamoDBRDFTripleIntegrityCheckerTests
    {
        [TestMethod]
        public async Task CheckIntegrityAsyncHandlesEmptyDatabase()
        {
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty);

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(0, report.RdfTriplesWithNoMatchingLiveInstance.Count);
            Assert.AreEqual(0, report.RdfTriplesWithNoMatchingTargetLiveInstance.Count);
            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncIdentifiesOrphanedProperties()
        {
            var orphanedProp = CreateRDFTriple("node1", "TestGraph#prop#UnknownType#prop1#value1#node1", "value1");
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(orphanedProp));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.RdfTriplesWithNoMatchingLiveInstance.Count);
            Assert.AreEqual("node1", report.RdfTriplesWithNoMatchingLiveInstance[0].Subject);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncAllowsValidNode()
        {
            var typeTriple = CreateRDFTriple("node1", new HasType("TestGraph", "TestNode", "node1").ToString(), "TestNode");
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(0, report.RdfTriplesWithNoMatchingLiveInstance.Count);
            Assert.AreEqual(0, report.RdfTriplesWithNoMatchingTargetLiveInstance.Count);
            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncDetectsInvalidNodeType()
        {
            var typeTriple = CreateRDFTriple("node1", new HasType("TestGraph", "InvalidNode", "node1").ToString(), "InvalidNode");
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.NodeIntegrityErrors.Count);
            Assert.IsTrue(report.NodeIntegrityErrors[0].Errors.Any(e => e.Message.Contains("InvalidNode")));
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncDetectsMissingInEdgeTarget()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var typeTriple = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var inEdge = CreateRDFTriple(subject1, $"TestGraph#in#TestNode#TestEdge#{subject1}#{subject2}", subject2);
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple).Add(inEdge));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.RdfTriplesWithNoMatchingTargetLiveInstance.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncDetectsMissingOutEdgeTarget()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var typeTriple = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var outEdge = CreateRDFTriple(subject1, $"TestGraph#out#TestNode#TestEdge#{subject2}#{subject1}", subject2);
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple).Add(outEdge));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.RdfTriplesWithNoMatchingTargetLiveInstance.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncDetectsCardinalityViolationTooMany()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var subject3 = "node3";
            var typeTriple1 = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var typeTriple2 = CreateRDFTriple(subject2, new HasType("TestGraph", "SourceNode", subject2).ToString(), "SourceNode");
            var typeTriple3 = CreateRDFTriple(subject3, new HasType("TestGraph", "SourceNode", subject3).ToString(), "SourceNode");
            var inEdge1 = CreateRDFTriple(subject1, $"TestGraph#in#TestNode#TestEdge#{subject1}#{subject2}", subject2);
            var inEdge2 = CreateRDFTriple(subject1, $"TestGraph#in#TestNode#TestEdge#{subject1}#{subject3}", subject3);

            var triples = ImmutableList<RDFTriple>.Empty
                .Add(typeTriple1)
                .Add(typeTriple2)
                .Add(typeTriple3)
                .Add(inEdge1)
                .Add(inEdge2);

            var edgeSchema = new EdgeSchema("TestEdge", "TestNode", EdgeCardinality.ZeroOrOne, "SourceNode", EdgeCardinality.ZeroOrOne);
            var (checker, _) = CreateChecker(triples, ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.NodeIntegrityErrors.Count);
            Assert.IsTrue(report.NodeIntegrityErrors[0].Errors.Any(e => e.Message.Contains("Too many")));
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncDetectsCardinalityViolationTooFew()
        {
            var subject1 = "node1";
            var typeTriple = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");

            var edgeSchema = new EdgeSchema("TestEdge", "TestNode", EdgeCardinality.One, "SourceNode", EdgeCardinality.One);
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple), ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.NodeIntegrityErrors.Count);
            Assert.IsTrue(report.NodeIntegrityErrors[0].Errors.Any(e => e.Message.Contains("Too few")));
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncAllowsZeroOrManyCardinality()
        {
            var subject1 = "node1";
            var typeTriple = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");

            var edgeSchema = new EdgeSchema("TestEdge", "TestNode", EdgeCardinality.ZeroOrMany, "TargetNode", EdgeCardinality.ZeroOrMany);
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple), ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncAllowsOneOrManyCardinalityWithOneEdge()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var typeTriple1 = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var typeTriple2 = CreateRDFTriple(subject2, new HasType("TestGraph", "TargetNode", subject2).ToString(), "TargetNode");
            var outEdge = CreateRDFTriple(subject1, $"TestGraph#out#TestNode#TestEdge#{subject2}#{subject1}", subject2);
            var inEdge = CreateRDFTriple(subject2, $"TestGraph#in#TargetNode#TestEdge#{subject2}#{subject1}", subject1);

            var triples = ImmutableList<RDFTriple>.Empty.Add(typeTriple1).Add(typeTriple2).Add(outEdge).Add(inEdge);
            var edgeSchema = new EdgeSchema("TestEdge", "TargetNode", EdgeCardinality.OneOrMany, "TestNode", EdgeCardinality.OneOrMany);
            var (checker, _) = CreateChecker(triples, ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public async Task ClearAllDataAsyncRemovesAllItems()
        {
            var triple1 = CreateRDFTriple("node1", "pred1", "obj1");
            var triple2 = CreateRDFTriple("node2", "pred2", "obj2");
            var (checker, mockClient) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(triple1).Add(triple2));

            await checker.ClearAllDataAsync(CancellationToken.None);

            Assert.AreEqual(0, mockClient.ItemCount);
        }

        [TestMethod]
        public async Task ClearAllDataAsyncHandlesEmptyTable()
        {
            var (checker, mockClient) = CreateChecker(ImmutableList<RDFTriple>.Empty);

            await checker.ClearAllDataAsync(CancellationToken.None);

            Assert.AreEqual(0, mockClient.ItemCount);
        }

        [TestMethod]
        public async Task RemoveRdfTriplesAsyncRemovesSpecifiedTriples()
        {
            var triple1 = CreateRDFTriple("node1", "pred1", "obj1");
            var triple2 = CreateRDFTriple("node2", "pred2", "obj2");
            var (checker, mockClient) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(triple1).Add(triple2));

            await checker.RemoveRdfTriplesAsync(ImmutableList<RDFTriple>.Empty.Add(triple1), CancellationToken.None);

            Assert.AreEqual(1, mockClient.ItemCount);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncDetectsMissingInEdgePropTarget()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var typeTriple = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var inEdgeProp = CreateRDFTriple(subject1, new HasInEdgeProp("TestGraph", "TestNode", "TestEdge", "prop1", "value", subject1, subject2).ToString(), "value");
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple).Add(inEdgeProp));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.RdfTriplesWithNoMatchingTargetLiveInstance.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncDetectsMissingOutEdgePropTarget()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var typeTriple = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var outEdgeProp = CreateRDFTriple(subject1, new HasOutEdgeProp("TestGraph", "TestNode", "TestEdge", "prop1", "value", subject2, subject1).ToString(), "value");
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple).Add(outEdgeProp));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.RdfTriplesWithNoMatchingTargetLiveInstance.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncAllowsZeroInEdgesForZeroOrManyCardinality()
        {
            var subject1 = "node1";
            var typeTriple = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");

            var edgeSchema = new EdgeSchema("TestEdge", "TestNode", EdgeCardinality.ZeroOrMany, "SourceNode", EdgeCardinality.ZeroOrMany);
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple), ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncAllowsMultipleInEdgesForZeroOrManyCardinality()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var subject3 = "node3";
            var typeTriple1 = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var typeTriple2 = CreateRDFTriple(subject2, new HasType("TestGraph", "SourceNode", subject2).ToString(), "SourceNode");
            var typeTriple3 = CreateRDFTriple(subject3, new HasType("TestGraph", "SourceNode", subject3).ToString(), "SourceNode");
            var inEdge1 = CreateRDFTriple(subject1, $"TestGraph#in#TestNode#TestEdge#{subject1}#{subject2}", subject2);
            var inEdge2 = CreateRDFTriple(subject1, $"TestGraph#in#TestNode#TestEdge#{subject1}#{subject3}", subject3);

            var triples = ImmutableList<RDFTriple>.Empty
                .Add(typeTriple1)
                .Add(typeTriple2)
                .Add(typeTriple3)
                .Add(inEdge1)
                .Add(inEdge2);

            var edgeSchema = new EdgeSchema("TestEdge", "TestNode", EdgeCardinality.ZeroOrMany, "SourceNode", EdgeCardinality.ZeroOrMany);
            var (checker, _) = CreateChecker(triples, ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncDetectsTooManyInEdgesForOneCardinality()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var subject3 = "node3";
            var typeTriple1 = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var typeTriple2 = CreateRDFTriple(subject2, new HasType("TestGraph", "SourceNode", subject2).ToString(), "SourceNode");
            var typeTriple3 = CreateRDFTriple(subject3, new HasType("TestGraph", "SourceNode", subject3).ToString(), "SourceNode");
            var inEdge1 = CreateRDFTriple(subject1, $"TestGraph#in#TestNode#TestEdge#{subject1}#{subject2}", subject2);
            var inEdge2 = CreateRDFTriple(subject1, $"TestGraph#in#TestNode#TestEdge#{subject1}#{subject3}", subject3);
            var outEdge1 = CreateRDFTriple(subject2, $"TestGraph#out#SourceNode#TestEdge#{subject1}#{subject2}", subject1);
            var outEdge2 = CreateRDFTriple(subject3, $"TestGraph#out#SourceNode#TestEdge#{subject1}#{subject3}", subject1);

            var triples = ImmutableList<RDFTriple>.Empty
                .Add(typeTriple1)
                .Add(typeTriple2)
                .Add(typeTriple3)
                .Add(inEdge1)
                .Add(inEdge2)
                .Add(outEdge1)
                .Add(outEdge2);

            var edgeSchema = new EdgeSchema("TestEdge", "TestNode", EdgeCardinality.One, "SourceNode", EdgeCardinality.One);
            var (checker, _) = CreateChecker(triples, ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.NodeIntegrityErrors.Count);
            Assert.IsTrue(report.NodeIntegrityErrors[0].Errors.Any(e => e.Message.Contains("Too many")));
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncAllowsSingleInEdgeForOneCardinality()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var typeTriple1 = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var typeTriple2 = CreateRDFTriple(subject2, new HasType("TestGraph", "SourceNode", subject2).ToString(), "SourceNode");
            var inEdge = CreateRDFTriple(subject1, $"TestGraph#in#TestNode#TestEdge#{subject1}#{subject2}", subject2);
            var outEdge = CreateRDFTriple(subject2, $"TestGraph#out#SourceNode#TestEdge#{subject1}#{subject2}", subject1);

            var triples = ImmutableList<RDFTriple>.Empty.Add(typeTriple1).Add(typeTriple2).Add(inEdge).Add(outEdge);
            var edgeSchema = new EdgeSchema("TestEdge", "TestNode", EdgeCardinality.One, "SourceNode", EdgeCardinality.One);
            var (checker, _) = CreateChecker(triples, ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncDetectsTooFewInEdgesForOneOrManyCardinality()
        {
            var subject1 = "node1";
            var typeTriple = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");

            var edgeSchema = new EdgeSchema("TestEdge", "TestNode", EdgeCardinality.OneOrMany, "SourceNode", EdgeCardinality.OneOrMany);
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple), ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.NodeIntegrityErrors.Count);
            Assert.IsTrue(report.NodeIntegrityErrors[0].Errors.Any(e => e.Message.Contains("Too few")));
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncAllowsMultipleInEdgesForOneOrManyCardinality()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var subject3 = "node3";
            var typeTriple1 = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var typeTriple2 = CreateRDFTriple(subject2, new HasType("TestGraph", "SourceNode", subject2).ToString(), "SourceNode");
            var typeTriple3 = CreateRDFTriple(subject3, new HasType("TestGraph", "SourceNode", subject3).ToString(), "SourceNode");
            var inEdge1 = CreateRDFTriple(subject1, $"TestGraph#in#TestNode#TestEdge#{subject1}#{subject2}", subject2);
            var inEdge2 = CreateRDFTriple(subject1, $"TestGraph#in#TestNode#TestEdge#{subject1}#{subject3}", subject3);
            var outEdge1 = CreateRDFTriple(subject2, $"TestGraph#out#SourceNode#TestEdge#{subject1}#{subject2}", subject1);
            var outEdge2 = CreateRDFTriple(subject3, $"TestGraph#out#SourceNode#TestEdge#{subject1}#{subject3}", subject1);

            var triples = ImmutableList<RDFTriple>.Empty
                .Add(typeTriple1)
                .Add(typeTriple2)
                .Add(typeTriple3)
                .Add(inEdge1)
                .Add(inEdge2)
                .Add(outEdge1)
                .Add(outEdge2);

            var edgeSchema = new EdgeSchema("TestEdge", "TestNode", EdgeCardinality.OneOrMany, "SourceNode", EdgeCardinality.OneOrMany);
            var (checker, _) = CreateChecker(triples, ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncDetectsTooManyOutEdgesForZeroOrOneCardinality()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var subject3 = "node3";
            var typeTriple1 = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var typeTriple2 = CreateRDFTriple(subject2, new HasType("TestGraph", "TargetNode", subject2).ToString(), "TargetNode");
            var typeTriple3 = CreateRDFTriple(subject3, new HasType("TestGraph", "TargetNode", subject3).ToString(), "TargetNode");
            var outEdge1 = CreateRDFTriple(subject1, $"TestGraph#out#TestNode#TestEdge#{subject2}#{subject1}", subject2);
            var outEdge2 = CreateRDFTriple(subject1, $"TestGraph#out#TestNode#TestEdge#{subject3}#{subject1}", subject3);

            var triples = ImmutableList<RDFTriple>.Empty
                .Add(typeTriple1)
                .Add(typeTriple2)
                .Add(typeTriple3)
                .Add(outEdge1)
                .Add(outEdge2);

            var edgeSchema = new EdgeSchema("TestEdge", "TargetNode", EdgeCardinality.ZeroOrOne, "TestNode", EdgeCardinality.ZeroOrOne);
            var (checker, _) = CreateChecker(triples, ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.NodeIntegrityErrors.Count);
            Assert.IsTrue(report.NodeIntegrityErrors[0].Errors.Any(e => e.Message.Contains("Too many")));
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncAllowsZeroOutEdgesForZeroOrManyCardinality()
        {
            var subject1 = "node1";
            var typeTriple = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");

            var edgeSchema = new EdgeSchema("TestEdge", "TargetNode", EdgeCardinality.ZeroOrMany, "TestNode", EdgeCardinality.ZeroOrMany);
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple), ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncAllowsMultipleOutEdgesForZeroOrManyCardinality()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var subject3 = "node3";
            var typeTriple1 = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var typeTriple2 = CreateRDFTriple(subject2, new HasType("TestGraph", "TargetNode", subject2).ToString(), "TargetNode");
            var typeTriple3 = CreateRDFTriple(subject3, new HasType("TestGraph", "TargetNode", subject3).ToString(), "TargetNode");
            var outEdge1 = CreateRDFTriple(subject1, $"TestGraph#out#TestNode#TestEdge#{subject2}#{subject1}", subject2);
            var outEdge2 = CreateRDFTriple(subject1, $"TestGraph#out#TestNode#TestEdge#{subject3}#{subject1}", subject3);

            var triples = ImmutableList<RDFTriple>.Empty
                .Add(typeTriple1)
                .Add(typeTriple2)
                .Add(typeTriple3)
                .Add(outEdge1)
                .Add(outEdge2);

            var edgeSchema = new EdgeSchema("TestEdge", "TargetNode", EdgeCardinality.ZeroOrMany, "TestNode", EdgeCardinality.ZeroOrMany);
            var (checker, _) = CreateChecker(triples, ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncDetectsTooManyOutEdgesForOneCardinality()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var subject3 = "node3";
            var typeTriple1 = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var typeTriple2 = CreateRDFTriple(subject2, new HasType("TestGraph", "TargetNode", subject2).ToString(), "TargetNode");
            var typeTriple3 = CreateRDFTriple(subject3, new HasType("TestGraph", "TargetNode", subject3).ToString(), "TargetNode");
            var outEdge1 = CreateRDFTriple(subject1, $"TestGraph#out#TestNode#TestEdge#{subject2}#{subject1}", subject2);
            var outEdge2 = CreateRDFTriple(subject1, $"TestGraph#out#TestNode#TestEdge#{subject3}#{subject1}", subject3);
            var inEdge1 = CreateRDFTriple(subject2, $"TestGraph#in#TargetNode#TestEdge#{subject2}#{subject1}", subject1);
            var inEdge2 = CreateRDFTriple(subject3, $"TestGraph#in#TargetNode#TestEdge#{subject3}#{subject1}", subject1);

            var triples = ImmutableList<RDFTriple>.Empty
                .Add(typeTriple1)
                .Add(typeTriple2)
                .Add(typeTriple3)
                .Add(outEdge1)
                .Add(outEdge2)
                .Add(inEdge1)
                .Add(inEdge2);

            var edgeSchema = new EdgeSchema("TestEdge", "TargetNode", EdgeCardinality.One, "TestNode", EdgeCardinality.One);
            var (checker, _) = CreateChecker(triples, ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.NodeIntegrityErrors.Count);
            Assert.IsTrue(report.NodeIntegrityErrors[0].Errors.Any(e => e.Message.Contains("Too many")));
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncAllowsSingleOutEdgeForOneCardinality()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var typeTriple1 = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var typeTriple2 = CreateRDFTriple(subject2, new HasType("TestGraph", "TargetNode", subject2).ToString(), "TargetNode");
            var outEdge = CreateRDFTriple(subject1, $"TestGraph#out#TestNode#TestEdge#{subject2}#{subject1}", subject2);
            var inEdge = CreateRDFTriple(subject2, $"TestGraph#in#TargetNode#TestEdge#{subject2}#{subject1}", subject1);

            var triples = ImmutableList<RDFTriple>.Empty.Add(typeTriple1).Add(typeTriple2).Add(outEdge).Add(inEdge);
            var edgeSchema = new EdgeSchema("TestEdge", "TargetNode", EdgeCardinality.One, "TestNode", EdgeCardinality.One);
            var (checker, _) = CreateChecker(triples, ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncDetectsTooFewOutEdgesForOneCardinality()
        {
            var subject1 = "node1";
            var typeTriple = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");

            var edgeSchema = new EdgeSchema("TestEdge", "TargetNode", EdgeCardinality.One, "TestNode", EdgeCardinality.One);
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple), ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.NodeIntegrityErrors.Count);
            Assert.IsTrue(report.NodeIntegrityErrors[0].Errors.Any(e => e.Message.Contains("Too few")));
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncDetectsTooFewOutEdgesForOneOrManyCardinality()
        {
            var subject1 = "node1";
            var typeTriple = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");

            var edgeSchema = new EdgeSchema("TestEdge", "TargetNode", EdgeCardinality.OneOrMany, "TestNode", EdgeCardinality.OneOrMany);
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple), ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(1, report.NodeIntegrityErrors.Count);
            Assert.IsTrue(report.NodeIntegrityErrors[0].Errors.Any(e => e.Message.Contains("Too few")));
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncAllowsMultipleOutEdgesForOneOrManyCardinality()
        {
            var subject1 = "node1";
            var subject2 = "node2";
            var subject3 = "node3";
            var typeTriple1 = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");
            var typeTriple2 = CreateRDFTriple(subject2, new HasType("TestGraph", "TargetNode", subject2).ToString(), "TargetNode");
            var typeTriple3 = CreateRDFTriple(subject3, new HasType("TestGraph", "TargetNode", subject3).ToString(), "TargetNode");
            var outEdge1 = CreateRDFTriple(subject1, $"TestGraph#out#TestNode#TestEdge#{subject2}#{subject1}", subject2);
            var outEdge2 = CreateRDFTriple(subject1, $"TestGraph#out#TestNode#TestEdge#{subject3}#{subject1}", subject3);
            var inEdge1 = CreateRDFTriple(subject2, $"TestGraph#in#TargetNode#TestEdge#{subject2}#{subject1}", subject1);
            var inEdge2 = CreateRDFTriple(subject3, $"TestGraph#in#TargetNode#TestEdge#{subject3}#{subject1}", subject1);

            var triples = ImmutableList<RDFTriple>.Empty
                .Add(typeTriple1)
                .Add(typeTriple2)
                .Add(typeTriple3)
                .Add(outEdge1)
                .Add(outEdge2)
                .Add(inEdge1)
                .Add(inEdge2);

            var edgeSchema = new EdgeSchema("TestEdge", "TargetNode", EdgeCardinality.OneOrMany, "TestNode", EdgeCardinality.OneOrMany);
            var (checker, _) = CreateChecker(triples, ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public async Task ClearAllDataAsyncHandlesMultipleBatches()
        {
            var triples = Enumerable.Range(1, 30)
                .Select(i => CreateRDFTriple($"node{i}", $"pred{i}", $"obj{i}"))
                .ToImmutableList();
            var (checker, mockClient) = CreateChecker(triples);

            await checker.ClearAllDataAsync(CancellationToken.None);

            Assert.AreEqual(0, mockClient.ItemCount);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncHandlesPaginationWithExclusiveStartKey()
        {
            var triples = Enumerable.Range(1, 1500)
                .Select(i => CreateRDFTriple($"node{i}", new HasType("TestGraph", "TestNode", $"node{i}").ToString(), "TestNode"))
                .ToImmutableList();
            var (checker, _) = CreateChecker(triples);

            var report = await checker.CheckIntegrityAsync(CancellationToken.None);

            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncThrowsForUnsupportedInEdgeCardinality()
        {
            var subject1 = "node1";
            var typeTriple = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");

            var edgeSchema = new EdgeSchema("TestEdge", "TestNode", (EdgeCardinality)999, "SourceNode", EdgeCardinality.One);
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple), ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            await Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
                await checker.CheckIntegrityAsync(CancellationToken.None));
        }

        [TestMethod]
        public async Task CheckIntegrityAsyncThrowsForUnsupportedOutEdgeCardinality()
        {
            var subject1 = "node1";
            var typeTriple = CreateRDFTriple(subject1, new HasType("TestGraph", "TestNode", subject1).ToString(), "TestNode");

            var edgeSchema = new EdgeSchema("TestEdge", "TargetNode", EdgeCardinality.One, "TestNode", (EdgeCardinality)999);
            var (checker, _) = CreateChecker(ImmutableList<RDFTriple>.Empty.Add(typeTriple), ImmutableList<EdgeSchema>.Empty.Add(edgeSchema));

            await Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
                await checker.CheckIntegrityAsync(CancellationToken.None));
        }

        private static RDFTriple CreateRDFTriple(string subject, string predicate, string obj)
        {
            return new RDFTriple(subject, predicate, obj, obj, "partition0", null);
        }

        private static (AmazonDynamoDBRDFTripleIntegrityChecker, MockDynamoDBClient) CreateChecker(
            ImmutableList<RDFTriple> initialData,
            ImmutableList<EdgeSchema>? edges = null)
        {
            var graphSettings = new GraphSettings("TestTable", "TestGraph", 1, "ByPredIndex", "ByObjIndex");
            var graphSettingsService = new MockGraphSettingsService(graphSettings);
            var keyService = new MockKeyService();
            var mockClient = new MockDynamoDBClient(initialData);
            var dataModelMapper = new MockDataModelMapper();

            var edgeSchemas = edges ?? ImmutableList<EdgeSchema>.Empty;

            // Define valid node types - only these are considered valid in the schema
            var validNodeTypes = ImmutableList<string>.Empty
                .Add("TestNode")
                .Add("SourceNode")
                .Add("TargetNode");

            var nodesByType = validNodeTypes.ToImmutableHashSet();
            var edgesByType = edgeSchemas.ToImmutableDictionary(e => e.Name);

            var graphSchema = new GraphSchema(validNodeTypes, nodesByType, edgeSchemas, edgesByType);
            var graphSchemaService = new MockGraphSchemaService(graphSchema);

            var checker = new AmazonDynamoDBRDFTripleIntegrityChecker(
                graphSettingsService,
                keyService,
                mockClient,
                dataModelMapper,
                graphSchemaService);

            return (checker, mockClient);
        }

        private sealed class MockGraphSettingsService(GraphSettings settings) : IGraphSettingsService
        {
            public GraphSettings GetGraphSettings() => settings;
        }

        private sealed class MockKeyService : IAmazonDynamoDBKeyService
        {
            public Task<ImmutableDictionary<string, AttributeValue>> CreateKeyMapAsync(
                string tableName,
                ImmutableDictionary<string, AttributeValue> attributeMap,
                CancellationToken cancellationToken)
            {
                var key = new Dictionary<string, AttributeValue>();
                if (attributeMap.TryGetValue("Subject", out var subjectValue))
                {
                    key["Subject"] = subjectValue;
                }
                if (attributeMap.TryGetValue("Predicate", out var predicateValue))
                {
                    key["Predicate"] = predicateValue;
                }
                if (attributeMap.TryGetValue("Partition", out var partitionValue))
                {
                    key["Partition"] = partitionValue;
                }
                return Task.FromResult(key.ToImmutableDictionary());
            }
        }

        private sealed class MockDynamoDBClient : IAmazonDynamoDB
        {
            private readonly List<RDFTriple> _items;

            public MockDynamoDBClient(ImmutableList<RDFTriple> initialItems)
            {
                _items = initialItems.ToList();
            }

            public int ItemCount => _items.Count;

            public Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var startIndex = 0;
                if (request.ExclusiveStartKey != null && request.ExclusiveStartKey.Count > 0)
                {
                    var startSubject = request.ExclusiveStartKey["Subject"].S;
                    startIndex = _items.FindIndex(i => i.Subject == startSubject) + 1;
                }

                var limit = request.Limit > 0 ? request.Limit : 1000;
                var itemsToReturn = _items.Skip(startIndex).Take(limit).ToList();

                var attributeMaps = itemsToReturn.Select(ToAttributeMap).ToList();

                Dictionary<string, AttributeValue>? lastKey = null;
                if (startIndex + limit < _items.Count)
                {
                    var lastItem = itemsToReturn.Last();
                    lastKey = new Dictionary<string, AttributeValue>
                    {
                        { "Subject", new AttributeValue { S = lastItem.Subject } },
                        { "Predicate", new AttributeValue { S = lastItem.Predicate } },
                        { "Partition", new AttributeValue { S = lastItem.Partition } }
                    };
                }

                return Task.FromResult(new ScanResponse
                {
                    Items = attributeMaps,
                    LastEvaluatedKey = lastKey ?? new Dictionary<string, AttributeValue>()
                });
            }

            public Task<BatchWriteItemResponse> BatchWriteItemAsync(
                BatchWriteItemRequest request,
                CancellationToken cancellationToken = default)
            {
                foreach (var tableRequests in request.RequestItems)
                {
                    foreach (var writeRequest in tableRequests.Value)
                    {
                        if (writeRequest.DeleteRequest != null)
                        {
                            var keyToDelete = writeRequest.DeleteRequest.Key;
                            _items.RemoveAll(item =>
                                item.Subject == keyToDelete["Subject"].S &&
                                item.Predicate == keyToDelete["Predicate"].S);
                        }
                    }
                }
                return Task.FromResult(new BatchWriteItemResponse());
            }

            private static Dictionary<string, AttributeValue> ToAttributeMap(RDFTriple triple)
            {
                var map = new Dictionary<string, AttributeValue>
                {
                    { "Subject", new AttributeValue { S = triple.Subject } },
                    { "Predicate", new AttributeValue { S = triple.Predicate } },
                    { "Object", new AttributeValue { S = triple.Object ?? "" } },
                    { "Partition", new AttributeValue { S = triple.Partition } }
                };
                if (triple.IndexedObject != null)
                {
                    map["IndexedObject"] = new AttributeValue { S = triple.IndexedObject };
                }
                return map;
            }

            // Unused interface members
            public Amazon.Runtime.IClientConfig Config => throw new System.NotImplementedException();
            public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, ReturnConsumedCapacity returnConsumedCapacity, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<BatchWriteItemResponse> BatchWriteItemAsync(Dictionary<string, List<WriteRequest>> requestItems, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<CreateGlobalTableResponse> CreateGlobalTableAsync(CreateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<CreateTableResponse> CreateTableAsync(string tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<CreateTableResponse> CreateTableAsync(CreateTableRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, ReturnValue returnValues, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DeleteItemResponse> DeleteItemAsync(DeleteItemRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DeleteTableResponse> DeleteTableAsync(string tableName, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DeleteTableResponse> DeleteTableAsync(DeleteTableRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeExportResponse> DescribeExportAsync(DescribeExportRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeGlobalTableResponse> DescribeGlobalTableAsync(DescribeGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeGlobalTableSettingsResponse> DescribeGlobalTableSettingsAsync(DescribeGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeImportResponse> DescribeImportAsync(DescribeImportRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeKinesisStreamingDestinationResponse> DescribeKinesisStreamingDestinationAsync(DescribeKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeTableResponse> DescribeTableAsync(string tableName, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeTableResponse> DescribeTableAsync(DescribeTableRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(Amazon.Runtime.AmazonWebServiceRequest request) => throw new System.NotImplementedException();
            public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, bool consistentRead, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ImportTableResponse> ImportTableAsync(ImportTableRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ListExportsResponse> ListExportsAsync(ListExportsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ListGlobalTablesResponse> ListGlobalTablesAsync(ListGlobalTablesRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ListImportsResponse> ListImportsAsync(ListImportsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, int limit, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(int limit, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ListTablesResponse> ListTablesAsync(ListTablesRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, ReturnValue returnValues, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ScanResponse> ScanAsync(string tableName, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactWriteItemsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, ReturnValue returnValues, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<UpdateItemResponse> UpdateItemAsync(UpdateItemRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();
            public Amazon.DynamoDBv2.Model.IDynamoDBv2PaginatorFactory Paginators => throw new System.NotImplementedException();
            public void Dispose() { }
        }

        private sealed class MockDataModelMapper : IAmazonDynamoDBRDFTripleItemService
        {
            public Dictionary<string, AttributeValue> ToAttributeMap(RDFTripleKey key)
            {
                return new Dictionary<string, AttributeValue>
                {
                    { "Subject", new AttributeValue { S = key.Subject } },
                    { "Predicate", new AttributeValue { S = key.Predicate } }
                };
            }

            public Dictionary<string, AttributeValue> ToAttributeMap(RDFTripleKeyWithPartition key)
            {
                return new Dictionary<string, AttributeValue>
                {
                    { "Subject", new AttributeValue { S = key.Subject } },
                    { "Predicate", new AttributeValue { S = key.Predicate } },
                    { "Partition", new AttributeValue { S = key.Partition } }
                };
            }

            public Dictionary<string, AttributeValue> ToAttributeMap(RDFTriple triple)
            {
                var map = new Dictionary<string, AttributeValue>
                {
                    { "Subject", new AttributeValue { S = triple.Subject } },
                    { "Predicate", new AttributeValue { S = triple.Predicate } },
                    { "Object", new AttributeValue { S = triple.Object ?? "" } },
                    { "Partition", new AttributeValue { S = triple.Partition } }
                };
                if (triple.IndexedObject != null)
                {
                    map["IndexedObject"] = new AttributeValue { S = triple.IndexedObject };
                }
                return map;
            }

            public bool IsRDFTriple(Dictionary<string, AttributeValue> value)
            {
                return value.ContainsKey("Subject") && value.ContainsKey("Predicate");
            }

            public RDFTriple ToRDFTriple(Dictionary<string, AttributeValue> attributeMap)
            {
                var subject = attributeMap["Subject"].S;
                var predicate = attributeMap["Predicate"].S;
                var obj = attributeMap.TryGetValue("Object", out var objAttr) ? objAttr.S : null;
                var indexedObject = attributeMap.TryGetValue("IndexedObject", out var idxAttr) ? idxAttr.S : "";
                var partition = attributeMap["Partition"].S;
                return new RDFTriple(subject, predicate, indexedObject ?? "", obj ?? "", partition, null);
            }
        }

        private sealed class MockGraphSchemaService(GraphSchema schema) : IGraphSchemaService
        {
            public Task<GraphSchema> GetGraphSchemaAsync(CancellationToken cancellationToken)
            {
                return Task.FromResult(schema);
            }
        }
    }
}
