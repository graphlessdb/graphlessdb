/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Domain.Graph;
using GraphlessDB.Domain.Graph.Services;
using GraphlessDB.Query.Datalog.Core;
using GraphlessDB.Query.Datalog.Results;
using GraphlessDB.Query.Services;

namespace GraphlessDB.Query.Datalog.Execution
{
    /// <summary>
    /// Executes datalog queries by compiling patterns and executing them
    /// </summary>
    internal sealed class DatalogQueryExecutor : IGraphQueryNodeExecutionService<DatalogQuery>
    {
        private readonly IGraphQueryService _graphQueryService;
        private readonly DatalogPatternCompiler _patternCompiler;
        private readonly DatalogRecursiveExecutor _recursiveExecutor;

        public DatalogQueryExecutor(
            IGraphQueryService graphQueryService,
            DatalogPatternCompiler patternCompiler,
            DatalogRecursiveExecutor recursiveExecutor)
        {
            _graphQueryService = graphQueryService;
            _patternCompiler = patternCompiler;
            _recursiveExecutor = recursiveExecutor;
        }

        public async Task<GraphExecutionContext> ExecuteAsync(
            GraphExecutionContext context,
            string key,
            CancellationToken cancellationToken)
        {
            var query = context.GetQuery<DatalogQuery>(key);

            // Step 1: Compile patterns into execution plan
            var plan = _patternCompiler.CompilePlan(query);

            // Step 2: Execute regular patterns
            var bindings = await ExecutePatternsAsync(
                plan.RegularPatterns,
                query.ConsistentRead,
                cancellationToken);

            // Step 3: Execute recursive patterns
            if (!query.RecursivePatterns.IsEmpty)
            {
                bindings = await _recursiveExecutor.ExecuteRecursivePatternsAsync(
                    query.RecursivePatterns,
                    bindings,
                    query.MaxRecursionDepth,
                    query.ConsistentRead,
                    cancellationToken);
            }

            // Step 4: Project results
            var result = ProjectResults(bindings, query.Projection);

            context = context.SetResult(key, result);
            return context;
        }

        public bool HasMoreChildData(GraphExecutionContext context, string key)
        {
            // Datalog queries are self-contained
            return false;
        }

        /// <summary>
        /// Executes regular (non-recursive) patterns to build bindings
        /// </summary>
        private async Task<BindingSet> ExecutePatternsAsync(
            ImmutableList<CompiledPattern> patterns,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var bindings = BindingSet.Empty;

            foreach (var pattern in patterns)
            {
                bindings = await ExecutePatternAsync(
                    pattern,
                    bindings,
                    consistentRead,
                    cancellationToken);
            }

            return bindings;
        }

        /// <summary>
        /// Executes a single compiled pattern
        /// </summary>
        private async Task<BindingSet> ExecutePatternAsync(
            CompiledPattern pattern,
            BindingSet currentBindings,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            return pattern switch
            {
                CompiledNodePattern nodePattern =>
                    await ExecuteNodePatternAsync(nodePattern, currentBindings, consistentRead, cancellationToken),
                CompiledEdgePattern edgePattern =>
                    await ExecuteEdgePatternAsync(edgePattern, currentBindings, consistentRead, cancellationToken),
                CompiledJoinPattern joinPattern =>
                    ExecuteJoinPattern(joinPattern, currentBindings),
                _ => throw new NotSupportedException($"Unknown pattern type: {pattern.GetType()}")
            };
        }

        /// <summary>
        /// Executes a node pattern by querying nodes of the specified type
        /// </summary>
        private async Task<BindingSet> ExecuteNodePatternAsync(
            CompiledNodePattern pattern,
            BindingSet currentBindings,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            GetConnectionResponse response;

            // Use pushdown if available
            if (pattern.PushdownData != null)
            {
                if (pattern.PushdownData.Values.Count == 1)
                {
                    var request = new GetConnectionByTypePropertyNameAndValueRequest(
                        TypeName: pattern.NodeTypeName,
                        PropertyName: pattern.PushdownData.PropertyName!,
                        PropertyOperator: ToPropertyOperator(pattern.PushdownData.Operator),
                        PropertyValue: pattern.PushdownData.Values[0],
                        OrderDesc: false,
                        ConnectionArguments: ConnectionArguments.FirstMax,
                        ConsistentRead: consistentRead);

                    response = await _graphQueryService.GetConnectionByTypePropertyNameAndValueAsync(
                        request,
                        cancellationToken);
                }
                else
                {
                    var request = new GetConnectionByTypePropertyNameAndValuesRequest(
                        TypeName: pattern.NodeTypeName,
                        PropertyName: pattern.PushdownData.PropertyName!,
                        PropertyOperator: ToPropertyOperator(pattern.PushdownData.Operator),
                        PropertyValues: pattern.PushdownData.Values,
                        OrderDesc: false,
                        ConnectionArguments: ConnectionArguments.FirstMax,
                        ConsistentRead: consistentRead);

                    response = await _graphQueryService.GetConnectionByTypePropertyNameAndValuesAsync(
                        request,
                        cancellationToken);
                }
            }
            else
            {
                // Query all nodes of type
                var request = new GetConnectionByTypeRequest(
                    TypeName: pattern.NodeTypeName,
                    ConnectionArguments: ConnectionArguments.FirstMax,
                    ConsistentRead: consistentRead);

                response = await _graphQueryService.GetConnectionByTypeAsync(request, cancellationToken);
            }

            // Create bindings for each node
            var newBindings = BindingSet.Empty;
            foreach (var edge in response.Connection.Edges)
            {
                var node = edge.Node;

                // Apply post-filters if needed
                if (pattern.Filter != null)
                {
                    // TODO: Implement post-filtering using GraphNodeFilterService
                    // For now, we'll skip post-filtering and rely on pushdown
                }

                // Create or extend binding
                var binding = ImmutableDictionary<string, IEntity>.Empty
                    .Add(pattern.VariableName, node);

                newBindings = newBindings.Add(binding);
            }

            // Join with existing bindings
            return currentBindings.Count == 0 ? newBindings : currentBindings.Join(newBindings);
        }

        /// <summary>
        /// Executes an edge pattern by traversing edges
        /// </summary>
        private async Task<BindingSet> ExecuteEdgePatternAsync(
            CompiledEdgePattern pattern,
            BindingSet currentBindings,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            // Check if either variable is already bound
            var fromBindings = currentBindings.GetBindings(pattern.FromVariableName).ToList();
            var toBindings = currentBindings.GetBindings(pattern.ToVariableName).ToList();

            if (fromBindings.Count > 0)
            {
                // From is bound - traverse outgoing edges
                return await TraverseFromBoundAsync(
                    pattern,
                    fromBindings,
                    currentBindings,
                    consistentRead,
                    cancellationToken);
            }
            else if (toBindings.Count > 0)
            {
                // To is bound - traverse incoming edges
                return await TraverseToBoundAsync(
                    pattern,
                    toBindings,
                    currentBindings,
                    consistentRead,
                    cancellationToken);
            }
            else
            {
                // Neither bound - query all edges (expensive)
                return await TraverseUnboundAsync(
                    pattern,
                    currentBindings,
                    consistentRead,
                    cancellationToken);
            }
        }

        /// <summary>
        /// Traverses edges when the FROM variable is bound
        /// </summary>
        private async Task<BindingSet> TraverseFromBoundAsync(
            CompiledEdgePattern pattern,
            System.Collections.Generic.List<System.Collections.Generic.KeyValuePair<string, IEntity>> fromBindings,
            BindingSet currentBindings,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var newBindings = BindingSet.Empty;

            foreach (var binding in fromBindings)
            {
                var fromNode = (INode)binding.Value;

                // Create connection with single node
                var nodeConnection = new Connection<RelayEdge<INode>, INode>(
                    ImmutableList.Create(new RelayEdge<INode>(string.Empty, fromNode)),
                    new PageInfo(false, false, string.Empty, string.Empty));

                var request = new ToEdgeQueryRequest(
                    NodeTypeName: fromNode.GetType().Name,
                    EdgeTypeName: pattern.EdgeTypeName,
                    NodeConnection: nodeConnection,
                    OrderBy: null,
                    FilterBy: null,
                    ConnectionArguments: ConnectionArguments.FirstMax,
                    ConsistentRead: consistentRead);

                var response = await _graphQueryService.GetOutToEdgeConnectionAsync(request, cancellationToken);

                // Get target nodes
                foreach (var edgeKey in response.Connection.Edges.Select(e => e.Node))
                {
                    var targetNode = await GetNodeByIdAsync(edgeKey.InId, consistentRead, cancellationToken);

                    if (targetNode != null)
                    {
                        var newBinding = ImmutableDictionary<string, IEntity>.Empty
                            .Add(pattern.FromVariableName, fromNode)
                            .Add(pattern.ToVariableName, targetNode);

                        newBindings = newBindings.Add(newBinding);
                    }
                }
            }

            return currentBindings.Join(newBindings);
        }

        /// <summary>
        /// Traverses edges when the TO variable is bound
        /// </summary>
        private async Task<BindingSet> TraverseToBoundAsync(
            CompiledEdgePattern pattern,
            System.Collections.Generic.List<System.Collections.Generic.KeyValuePair<string, IEntity>> toBindings,
            BindingSet currentBindings,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var newBindings = BindingSet.Empty;

            foreach (var binding in toBindings)
            {
                var toNode = (INode)binding.Value;

                // Create connection with single node
                var nodeConnection = new Connection<RelayEdge<INode>, INode>(
                    ImmutableList.Create(new RelayEdge<INode>(string.Empty, toNode)),
                    new PageInfo(false, false, string.Empty, string.Empty));

                var request = new ToEdgeQueryRequest(
                    NodeTypeName: toNode.GetType().Name,
                    EdgeTypeName: pattern.EdgeTypeName,
                    NodeConnection: nodeConnection,
                    OrderBy: null,
                    FilterBy: null,
                    ConnectionArguments: ConnectionArguments.FirstMax,
                    ConsistentRead: consistentRead);

                var response = await _graphQueryService.GetInToEdgeConnectionAsync(request, cancellationToken);

                // Get source nodes
                foreach (var edgeKey in response.Connection.Edges.Select(e => e.Node))
                {
                    var sourceNode = await GetNodeByIdAsync(edgeKey.OutId, consistentRead, cancellationToken);

                    if (sourceNode != null)
                    {
                        var newBinding = ImmutableDictionary<string, IEntity>.Empty
                            .Add(pattern.FromVariableName, sourceNode)
                            .Add(pattern.ToVariableName, toNode);

                        newBindings = newBindings.Add(newBinding);
                    }
                }
            }

            return currentBindings.Join(newBindings);
        }

        /// <summary>
        /// Traverses edges when neither variable is bound (expensive operation)
        /// </summary>
        private async Task<BindingSet> TraverseUnboundAsync(
            CompiledEdgePattern pattern,
            BindingSet currentBindings,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            // This is expensive - we need to query all edges of this type
            // For now, we'll throw an exception as this should be avoided by query optimization
            throw new NotSupportedException(
                "Edge patterns with both variables unbound are not supported. " +
                "Ensure at least one node pattern binds a variable before the edge pattern.");
        }

        /// <summary>
        /// Executes a join pattern
        /// </summary>
        private static BindingSet ExecuteJoinPattern(
            CompiledJoinPattern pattern,
            BindingSet currentBindings)
        {
            // Join is a filter operation - it filters binding rows where variables don't match
            // This is already handled by BindingSet.Join, so this is a no-op
            return currentBindings;
        }

        /// <summary>
        /// Projects results based on the projection specification
        /// </summary>
        private static GraphResult ProjectResults(BindingSet bindings, ProjectionSpec? projection)
        {
            if (projection == null || projection.Variables.IsEmpty)
            {
                // No projection specified - return all bindings
                return new DatalogConnectionResultMulti(
                    new Connection<RelayEdge<DatalogBindings>, DatalogBindings>(
                        bindings.Bindings.Select(b => new RelayEdge<DatalogBindings>(
                            string.Empty,
                            new DatalogBindings(b))).ToImmutableList(),
                        new PageInfo(false, false, string.Empty, string.Empty)),
                    null,
                    string.Empty,
                    false,
                    false);
            }

            if (projection.Variables.Count == 1)
            {
                // Single variable projection
                var variable = projection.Variables[0];
                var entities = bindings.Bindings
                    .Where(b => b.ContainsKey(variable.VariableName))
                    .Select(b => b[variable.VariableName])
                    .Distinct()
                    .ToImmutableList();

                // For now, return a simple result without pagination
                // TODO: Implement proper pagination support
                return new DatalogConnectionResultSingle(
                    entities,
                    null,
                    string.Empty,
                    false,
                    false);
            }
            else
            {
                // Multi-variable projection
                var datalogBindings = bindings.Bindings
                    .Select(b => new DatalogBindings(b))
                    .ToImmutableList();

                return new DatalogConnectionResultMulti(
                    new Connection<RelayEdge<DatalogBindings>, DatalogBindings>(
                        datalogBindings.Select(b => new RelayEdge<DatalogBindings>(string.Empty, b)).ToImmutableList(),
                        new PageInfo(false, false, string.Empty, string.Empty)),
                    null,
                    string.Empty,
                    false,
                    false);
            }
        }

        /// <summary>
        /// Gets a node by ID
        /// </summary>
        private async Task<INode?> GetNodeByIdAsync(
            string nodeId,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var request = new TryGetNodesRequest(
                ImmutableList.Create(nodeId),
                consistentRead);

            var response = await _graphQueryService.TryGetNodesAsync(request, cancellationToken);

            return response.Nodes.FirstOrDefault()?.Node;
        }

        /// <summary>
        /// Converts internal PropertyOperator to Storage PropertyOperator
        /// </summary>
        private static Storage.PropertyOperator ToPropertyOperator(PropertyOperator op)
        {
            return op switch
            {
                PropertyOperator.Equals => Storage.PropertyOperator.Equals,
                PropertyOperator.StartsWith => Storage.PropertyOperator.StartsWith,
                _ => throw new NotSupportedException($"Unknown property operator: {op}")
            };
        }
    }
}
