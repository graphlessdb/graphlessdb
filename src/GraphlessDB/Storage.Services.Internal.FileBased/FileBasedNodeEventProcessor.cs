/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;
using GraphlessDB.Graph.Services;

namespace GraphlessDB.Storage.Services.Internal.FileBased
{
    internal sealed class FileBasedNodeEventProcessor(
        IGraphSettingsService graphOptionsProvider,
        IGraphEventService nodeEventService,
        IFileBasedRDFEventReader rdfEventHandler,
        IRDFTripleFactory rdfTripleFactory,
        IRDFTripleStore rdfTripleStore) : IFileBasedNodeEventProcessor
    {
        public async Task ProcessFileBasedNodeEventsAsync(CancellationToken cancellationToken)
        {
            var options = graphOptionsProvider.GetGraphSettings();
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var rdfTriples = rdfEventHandler.DequeueRDFTripleEvents();
                if (rdfTriples.IsEmpty)
                {
                    return;
                }

                foreach (var rdfTriple in rdfTriples)
                {
                    if (rdfTriple.Predicate.StartsWith(HasType.ByGraphName(options.GraphName), StringComparison.Ordinal))
                    {
                        if (rdfTriple.VersionDetail == null)
                        {
                            throw new GraphlessDBOperationException("Expected VersionDetail");
                        }

                        var isNew = rdfTriple.VersionDetail.NodeVersion == 0;
                        if (isNew)
                        {
                            var node = rdfTripleFactory.GetNode(rdfTriple);
                            await nodeEventService.OnNodeEventAsync(new NodeEvent(node, null), cancellationToken);
                        }
                        else
                        {
                            var node = rdfTripleFactory.GetNode(rdfTriple);
                            var predicate = HasType.Parse(rdfTriple.Predicate);
                            var oldNodes = await rdfTripleStore.GetRDFTriplesAsync(
                                new GetRDFTriplesRequest(
                                    options.TableName,
                                    [new RDFTripleKey(rdfTriple.Subject, new HasBlob(predicate.GraphName, predicate.TypeName, rdfTriple.VersionDetail.NodeVersion - 1).ToString())],
                                    true), cancellationToken);

                            var oldNode = oldNodes
                                .Items
                                .Select(r => AsVersionedNode(r ?? throw new GraphlessDBOperationException("Expected RDFTriple")))
                                .Single();

                            await nodeEventService.OnNodeEventAsync(new NodeEvent(node, oldNode), cancellationToken);
                        }
                    }
                }
            }
        }

        private INode AsVersionedNode(RDFTriple value)
        {
            var predicate = HasBlob.Parse(value.Predicate);
            var versionDetail = new VersionDetail(predicate.Version, 0);
            var rdfTriple = new RDFTriple(value.Subject, value.Predicate, value.IndexedObject, value.Object, value.Partition, versionDetail);
            return rdfTripleFactory.GetNode(rdfTriple);
        }
    }
}
