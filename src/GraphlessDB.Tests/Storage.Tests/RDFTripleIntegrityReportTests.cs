/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using GraphlessDB.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Storage.Tests
{
    [TestClass]
    public sealed class RDFTripleIntegrityReportTests
    {
        [TestMethod]
        public void CanCreateEmptyReport()
        {
            var report = RDFTripleIntegrityReport.Empty;

            Assert.IsNotNull(report);
            Assert.AreEqual(0, report.RdfTriplesWithNoMatchingLiveInstance.Count);
            Assert.AreEqual(0, report.RdfTriplesWithNoMatchingTargetLiveInstance.Count);
            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public void CanCreateReportWithRdfTriplesWithNoMatchingLiveInstance()
        {
            var triple = new RDFTriple("subject1", "predicate1", "indexedObject1", "object1", "partition1", null);
            var triples = ImmutableList.Create(triple);

            var report = new RDFTripleIntegrityReport(triples, [], []);

            Assert.AreEqual(1, report.RdfTriplesWithNoMatchingLiveInstance.Count);
            Assert.AreEqual(triple, report.RdfTriplesWithNoMatchingLiveInstance[0]);
            Assert.AreEqual(0, report.RdfTriplesWithNoMatchingTargetLiveInstance.Count);
            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public void CanCreateReportWithRdfTriplesWithNoMatchingTargetLiveInstance()
        {
            var triple = new RDFTriple("subject1", "predicate1", "indexedObject1", "object1", "partition1", null);
            var triples = ImmutableList.Create(triple);

            var report = new RDFTripleIntegrityReport([], triples, []);

            Assert.AreEqual(0, report.RdfTriplesWithNoMatchingLiveInstance.Count);
            Assert.AreEqual(1, report.RdfTriplesWithNoMatchingTargetLiveInstance.Count);
            Assert.AreEqual(triple, report.RdfTriplesWithNoMatchingTargetLiveInstance[0]);
            Assert.AreEqual(0, report.NodeIntegrityErrors.Count);
        }

        [TestMethod]
        public void CanCreateReportWithNodeIntegrityErrors()
        {
            var exception = new InvalidOperationException("Test error");
            var errors = ImmutableList.Create<Exception>(exception);
            var nodeIntegrity = new NodeIntegrity("TypeName1", "Subject1", errors);
            var nodeIntegrityList = ImmutableList.Create(nodeIntegrity);

            var report = new RDFTripleIntegrityReport([], [], nodeIntegrityList);

            Assert.AreEqual(0, report.RdfTriplesWithNoMatchingLiveInstance.Count);
            Assert.AreEqual(0, report.RdfTriplesWithNoMatchingTargetLiveInstance.Count);
            Assert.AreEqual(1, report.NodeIntegrityErrors.Count);
            Assert.AreEqual(nodeIntegrity, report.NodeIntegrityErrors[0]);
        }

        [TestMethod]
        public void CanCreateReportWithAllFields()
        {
            var triple1 = new RDFTriple("subject1", "predicate1", "indexedObject1", "object1", "partition1", null);
            var triple2 = new RDFTriple("subject2", "predicate2", "indexedObject2", "object2", "partition2", null);
            var triples1 = ImmutableList.Create(triple1);
            var triples2 = ImmutableList.Create(triple2);

            var exception = new InvalidOperationException("Test error");
            var errors = ImmutableList.Create<Exception>(exception);
            var nodeIntegrity = new NodeIntegrity("TypeName1", "Subject1", errors);
            var nodeIntegrityList = ImmutableList.Create(nodeIntegrity);

            var report = new RDFTripleIntegrityReport(triples1, triples2, nodeIntegrityList);

            Assert.AreEqual(1, report.RdfTriplesWithNoMatchingLiveInstance.Count);
            Assert.AreEqual(triple1, report.RdfTriplesWithNoMatchingLiveInstance[0]);
            Assert.AreEqual(1, report.RdfTriplesWithNoMatchingTargetLiveInstance.Count);
            Assert.AreEqual(triple2, report.RdfTriplesWithNoMatchingTargetLiveInstance[0]);
            Assert.AreEqual(1, report.NodeIntegrityErrors.Count);
            Assert.AreEqual(nodeIntegrity, report.NodeIntegrityErrors[0]);
        }

        [TestMethod]
        public void CanCompareEqualReports()
        {
            var report1 = RDFTripleIntegrityReport.Empty;
            var report2 = RDFTripleIntegrityReport.Empty;

            Assert.AreEqual(report1, report2);
        }

        [TestMethod]
        public void CanCompareNonEqualReports()
        {
            var triple = new RDFTriple("subject1", "predicate1", "indexedObject1", "object1", "partition1", null);
            var triples = ImmutableList.Create(triple);

            var report1 = RDFTripleIntegrityReport.Empty;
            var report2 = new RDFTripleIntegrityReport(triples, [], []);

            Assert.AreNotEqual(report1, report2);
        }
    }
}
