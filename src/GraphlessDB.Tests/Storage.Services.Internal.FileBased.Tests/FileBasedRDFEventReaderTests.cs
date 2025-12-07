/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Storage;
using GraphlessDB.Storage.Services.Internal.FileBased;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Storage.Services.Internal.FileBased.Tests
{
    [TestClass]
    public sealed class FileBasedRDFEventReaderTests
    {
        private const string GraphName = "TestGraph";

        private static FileBasedRDFEventReader CreateEventReader()
        {
            var graphOptions = Options.Create(new GraphOptions
            {
                TableName = "TestTable",
                GraphName = GraphName,
                PartitionCount = 1
            });

            return new FileBasedRDFEventReader(graphOptions);
        }

        [TestMethod]
        public void DequeueRDFTripleEventsReturnsEmptyListInitially()
        {
            var eventReader = CreateEventReader();

            var events = eventReader.DequeueRDFTripleEvents();

            Assert.AreEqual(0, events.Count);
        }

        [TestMethod]
        public void OnRDFTripleAddedQueuesHasTypeTriple()
        {
            var eventReader = CreateEventReader();
            var predicate = HasType.ByGraphName(GraphName);
            var triple = new RDFTriple("subject1", predicate, "indexed", "object", "0", new VersionDetail(0, 0));

            eventReader.OnRDFTripleAdded(triple);

            var events = eventReader.DequeueRDFTripleEvents();
            Assert.AreEqual(1, events.Count);
            Assert.AreEqual(triple.Subject, events[0].Subject);
        }

        [TestMethod]
        public void OnRDFTripleUpdatedQueuesHasTypeTriple()
        {
            var eventReader = CreateEventReader();
            var predicate = HasType.ByGraphName(GraphName);
            var triple = new RDFTriple("subject1", predicate, "indexed", "object", "0", new VersionDetail(1, 0));

            eventReader.OnRDFTripleUpdated(triple);

            var events = eventReader.DequeueRDFTripleEvents();
            Assert.AreEqual(1, events.Count);
            Assert.AreEqual(triple.Subject, events[0].Subject);
        }

        [TestMethod]
        public void OnRDFTripleAddedIgnoresNonHasTypeTriple()
        {
            var eventReader = CreateEventReader();
            var triple = new RDFTriple("subject1", "predicate1", "indexed", "object", "0", null);

            eventReader.OnRDFTripleAdded(triple);

            var events = eventReader.DequeueRDFTripleEvents();
            Assert.AreEqual(0, events.Count);
        }

        [TestMethod]
        public void DequeueRDFTripleEventsClearsQueue()
        {
            var eventReader = CreateEventReader();
            var predicate = HasType.ByGraphName(GraphName);
            var triple = new RDFTriple("subject1", predicate, "indexed", "object", "0", new VersionDetail(0, 0));
            eventReader.OnRDFTripleAdded(triple);

            var firstDequeue = eventReader.DequeueRDFTripleEvents();
            var secondDequeue = eventReader.DequeueRDFTripleEvents();

            Assert.AreEqual(1, firstDequeue.Count);
            Assert.AreEqual(0, secondDequeue.Count);
        }
    }
}
