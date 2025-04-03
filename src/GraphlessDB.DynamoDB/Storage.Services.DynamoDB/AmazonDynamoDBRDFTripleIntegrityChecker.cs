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

namespace GraphlessDB.Storage.Services.DynamoDB
{
    internal sealed class AmazonDynamoDBRDFTripleIntegrityChecker(
        IGraphSettingsService graphOptionsProvider,
        IAmazonDynamoDBKeyService keyService,
        IAmazonDynamoDB client,
        IAmazonDynamoDBRDFTripleItemService dataModelMapper,
        IGraphSchemaService graphSchemaService) : IRDFTripleIntegrityChecker
    {
        public async Task<RDFTripleIntegrityReport> CheckIntegrityAsync(CancellationToken cancellationToken)
        {
            var rdfTriples = await GetRdfTriplesAsync(cancellationToken);
            var rdfTriplesBySubject = rdfTriples
                .GroupBy(k => k.Subject)
                .ToImmutableDictionary(k => k.Key, v => v.ToImmutableList());

            var liveSubjects = rdfTriples
                .Where(v => HasType.IsPredicate(v.Predicate))
                .Select(v => v.Subject)
                .ToImmutableHashSet();

            var rdfTriplesWithNoMatchingLiveInstance = rdfTriplesBySubject
                .Where(v => HasPropsOrEdgesButNotLive(v.Value))
                .SelectMany(v => v.Value.Where(vv => IsPropOrEdgePredicateType(vv.Predicate)))
                .ToImmutableList();

            var rdfTriplesWithNoMatchingTargetLiveInstance = rdfTriplesBySubject
                .SelectMany(v => v.Value)
                .Where(v => IsEdgeButNoLiveTarget(v, liveSubjects))
                .ToImmutableList();

            // NOTE Probably should check matching in and out edges also
            var graphSchema = await graphSchemaService.GetGraphSchemaAsync(cancellationToken);

            var nodeIntegrityErrors = liveSubjects
                .Select(v => GetNodeIntegrity(rdfTriplesBySubject[v], graphSchema))
                .Where(v => !v.Errors.IsEmpty)
                .ToImmutableList();

            return new RDFTripleIntegrityReport(
                rdfTriplesWithNoMatchingLiveInstance,
                rdfTriplesWithNoMatchingTargetLiveInstance,
                nodeIntegrityErrors
            );
        }

        public async Task ClearAllDataAsync(CancellationToken cancellationToken)
        {
            var options = graphOptionsProvider.GetGraphSettings();

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var scanRequest = new ScanRequest { TableName = options.TableName, Limit = 25 };
                var scanResponse = await client.ScanAsync(scanRequest, cancellationToken);
                if (scanResponse.Items.Count == 0)
                {
                    return;
                }

                var writeRequests = await Task.WhenAll(
                    scanResponse
                        .Items
                        .Select(async item => new WriteRequest
                        {
                            DeleteRequest = new DeleteRequest
                            {
                                Key = (await keyService.CreateKeyMapAsync(scanRequest.TableName, item.ToImmutableDictionary(), cancellationToken)).ToDictionary(k => k.Key, v => v.Value)
                            }
                        }));

                await client.BatchWriteItemAsync(new BatchWriteItemRequest
                {
                    RequestItems = new Dictionary<string, List<WriteRequest>>{
                        { scanRequest.TableName, writeRequests.ToList()}
                    }
                }, cancellationToken);
            }
        }

        public async Task RemoveRdfTriplesAsync(ImmutableList<RDFTriple> values, CancellationToken cancellationToken)
        {
            var options = graphOptionsProvider.GetGraphSettings();
            await client.BatchWriteItemAsync(
                new BatchWriteItemRequest(
                new Dictionary<string, List<WriteRequest>> {                        {
                    options.TableName, values.Select(item => new WriteRequest(new DeleteRequest(dataModelMapper.ToAttributeMap(item.AsKey())))).ToList()
                }}),
                BatchWriteItemOptions.Default,
                cancellationToken);
        }

        private static NodeIntegrity GetNodeIntegrity(ImmutableList<RDFTriple> nodeRdfTriples, GraphSchema graphSchema)
        {
            var nodeSubject = nodeRdfTriples
                .Select(v => v.Subject)
                .First();

            var nodeType = nodeRdfTriples
                .Select(v => Predicate.ParseTypeName(v.Predicate))
                .First();

            if (!graphSchema.NodesByType.Contains(nodeType))
            {
                return new NodeIntegrity(
                    nodeType,
                    nodeSubject,
                    [new InvalidOperationException($"NodeType '{nodeType}' is not valid")]);
            }

            var nodeInEdgeTypes = graphSchema.Edges.Where(e => e.NodeInType == nodeType).ToImmutableList();
            var nodeInEdgeTypeErrors = nodeInEdgeTypes
                .SelectMany(nodeInEdgeType =>
                {
                    var edgeRdfTriples = nodeRdfTriples
                        .Where(t => HasInEdge.IsPredicate(t.Predicate))
                        .Select(t => HasInEdge.Parse(t.Predicate))
                        .Where(t => t.EdgeTypeName == nodeInEdgeType.Name)
                        .ToImmutableList();

                    var edgeCount = edgeRdfTriples.Count;
                    switch (nodeInEdgeType.NodeOutCardinality)
                    {
                        case EdgeCardinality.ZeroOrOne:
                            if (edgeCount > 1)
                            {
                                return [(new InvalidOperationException($"Too many '{nodeInEdgeType.Name}' edges"))];
                            }

                            return [];
                        case EdgeCardinality.ZeroOrMany:
                            return [];
                        case EdgeCardinality.One:
                            if (edgeCount > 1)
                            {
                                return [(new InvalidOperationException($"Too many '{nodeInEdgeType.Name}' edges"))];
                            }

                            if (edgeCount < 1)
                            {
                                return [(new InvalidOperationException($"Too few '{nodeInEdgeType.Name}' edges"))];
                            }

                            return [];
                        case EdgeCardinality.OneOrMany:
                            if (edgeCount < 1)
                            {
                                return [(new InvalidOperationException($"Too few '{nodeInEdgeType.Name}' edges"))];
                            }

                            return ImmutableList<Exception>.Empty;
                        default:
                            throw new NotSupportedException();
                    }
                })
                .ToImmutableList();

            var nodeOutEdgeTypes = graphSchema.Edges.Where(e => e.NodeOutType == nodeType).ToImmutableList();
            var nodeOutEdgeTypeErrors = nodeOutEdgeTypes
                .SelectMany(nodeOutEdgeType =>
                {
                    var edgeRdfTriples = nodeRdfTriples
                        .Where(t => HasOutEdge.IsPredicate(t.Predicate))
                        .Select(t => HasOutEdge.Parse(t.Predicate))
                        .Where(t => t.EdgeTypeName == nodeOutEdgeType.Name)
                        .ToImmutableList();

                    var edgeCount = edgeRdfTriples.Count;
                    switch (nodeOutEdgeType.NodeInCardinality)
                    {
                        case EdgeCardinality.ZeroOrOne:
                            if (edgeCount > 1)
                            {
                                return [(new InvalidOperationException($"Too many '{nodeOutEdgeType.Name}' edges"))];
                            }

                            return [];
                        case EdgeCardinality.ZeroOrMany:
                            return [];
                        case EdgeCardinality.One:
                            if (edgeCount > 1)
                            {
                                return [(new InvalidOperationException($"Too many '{nodeOutEdgeType.Name}' edges"))];
                            }

                            if (edgeCount < 1)
                            {
                                return [(new InvalidOperationException($"Too few '{nodeOutEdgeType.Name}' edges"))];
                            }

                            return [];
                        case EdgeCardinality.OneOrMany:
                            if (edgeCount < 1)
                            {
                                return [(new InvalidOperationException($"Too few '{nodeOutEdgeType.Name}' edges"))];
                            }

                            return ImmutableList<Exception>.Empty;
                        default:
                            throw new NotSupportedException();
                    }
                })
                .ToImmutableList();

            return new NodeIntegrity(nodeType, nodeSubject, nodeInEdgeTypeErrors.AddRange(nodeOutEdgeTypeErrors));
        }

        private static bool IsEdgeButNoLiveTarget(RDFTriple value, ImmutableHashSet<string> subjects)
        {
            if (HasInEdge.IsPredicate(value.Predicate))
            {
                var edge = HasInEdge.Parse(value.Predicate);
                return !subjects.Contains(edge.NodeOutId);
            }

            if (HasInEdgeProp.IsPredicate(value.Predicate))
            {
                var edge = HasInEdgeProp.Parse(value.Predicate);
                return !subjects.Contains(edge.NodeOutId);
            }

            if (HasOutEdge.IsPredicate(value.Predicate))
            {
                var edge = HasOutEdge.Parse(value.Predicate);
                return !subjects.Contains(edge.NodeInId);
            }

            if (HasOutEdgeProp.IsPredicate(value.Predicate))
            {
                var edge = HasOutEdgeProp.Parse(value.Predicate);
                return !subjects.Contains(edge.NodeInId);
            }

            return false;
        }

        private static bool HasPropsOrEdgesButNotLive(ImmutableList<RDFTriple> rdfTriples)
        {
            var hasType = rdfTriples.Any(v => HasType.IsPredicate(v.Predicate));
            var hasPropOrEdge = rdfTriples.Any(v => IsPropOrEdgePredicateType(v.Predicate));
            return !hasType && hasPropOrEdge;
        }

        private static bool IsPropOrEdgePredicateType(string predicate)
        {
            return HasInEdge.IsPredicate(predicate) ||
                HasInEdgeProp.IsPredicate(predicate) ||
                HasOutEdge.IsPredicate(predicate) ||
                HasOutEdgeProp.IsPredicate(predicate) ||
                HasProp.IsPredicate(predicate);
        }

        private async Task<ImmutableList<RDFTriple>> GetRdfTriplesAsync(
            CancellationToken cancellationToken)
        {
            var options = graphOptionsProvider.GetGraphSettings();
            var exclusiveStartKey = null as Dictionary<string, AttributeValue>;
            var rdfTriples = ImmutableList<RDFTriple>.Empty;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var request = new ScanRequest(options.TableName)
                {
                    AttributesToGet = [
                        nameof(RDFTriple.Subject),
                        nameof(RDFTriple.Predicate),
                        nameof(RDFTriple.IndexedObject),
                        nameof(RDFTriple.Object),
                        nameof(RDFTriple.Partition),
                ],
                    Limit = 1000,
                    ExclusiveStartKey = exclusiveStartKey,
                };

                if (exclusiveStartKey != null)
                {
                    Console.WriteLine($"ExclusiveStartKey={exclusiveStartKey[nameof(RDFTriple.Subject)].S}");
                }

                var response = await client.ScanAsync(request, cancellationToken);
                if (response == null)
                {
                    break;
                }

                var newRdfTriples = response
                    .Items
                    .Select(dataModelMapper.ToRDFTriple);

                rdfTriples = rdfTriples.AddRange(newRdfTriples);
                if (response.LastEvaluatedKey.Count == 0)
                {
                    break;
                }

                exclusiveStartKey = response.LastEvaluatedKey;
            }

            return rdfTriples;
        }
    }
}