/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Domain.Graph;
using GraphlessDB.Domain.Graph.Services;
using GraphlessDB.Query.Datalog.Core;

namespace GraphlessDB.Query.Datalog.Execution
{
    /// <summary>
    /// Executes recursive/transitive closure patterns
    /// </summary>
    internal sealed class DatalogRecursiveExecutor
    {
        private readonly IGraphQueryService _graphQueryService;
        private readonly IGraphCursorSerializationService _cursorSerializer;
        private const int DefaultMaxDepth = 100;

        public DatalogRecursiveExecutor(
            IGraphQueryService graphQueryService,
            IGraphCursorSerializationService cursorSerializer)
        {
            _graphQueryService = graphQueryService;
            _cursorSerializer = cursorSerializer;
        }

        /// <summary>
        /// Executes all recursive patterns and returns extended bindings
        /// </summary>
        public async Task<BindingSet> ExecuteRecursivePatternsAsync(
            ImmutableList<RecursivePattern> patterns,
            BindingSet initialBindings,
            int? maxDepth,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var allBindings = initialBindings;

            foreach (var pattern in patterns)
            {
                var recursiveBindings = await ExecuteRecursivePatternAsync(
                    pattern,
                    allBindings,
                    maxDepth,
                    consistentRead,
                    cancellationToken);

                allBindings = allBindings.Union(recursiveBindings);
            }

            return allBindings;
        }

        /// <summary>
        /// Executes a single recursive pattern
        /// </summary>
        private async Task<BindingSet> ExecuteRecursivePatternAsync(
            RecursivePattern pattern,
            BindingSet startBindings,
            int? globalMaxDepth,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var maxDepth = pattern.MaxDepth == RecursivePattern.UnboundedDepth
                ? globalMaxDepth ?? DefaultMaxDepth
                : pattern.MaxDepth;

            return pattern.Mode switch
            {
                RecursionMode.BreadthFirst =>
                    await BreadthFirstSearchAsync(pattern, startBindings, maxDepth, consistentRead, cancellationToken),
                RecursionMode.DepthFirst =>
                    await DepthFirstSearchAsync(pattern, startBindings, maxDepth, consistentRead, cancellationToken),
                _ => throw new System.NotSupportedException($"Unknown recursion mode: {pattern.Mode}")
            };
        }

        /// <summary>
        /// Breadth-first search for recursive pattern execution
        /// </summary>
        private async Task<BindingSet> BreadthFirstSearchAsync(
            RecursivePattern pattern,
            BindingSet startBindings,
            int maxDepth,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var resultBindings = BindingSet.Empty;
            var visited = new HashSet<string>();
            var currentLevel = new List<INode>();

            // Initialize with start nodes
            foreach (var binding in startBindings.GetBindings(pattern.StartVariable.Name))
            {
                var node = (INode)binding.Value;
                if (visited.Add(node.Id))
                {
                    currentLevel.Add(node);
                }
            }

            int depth = 0;
            while (currentLevel.Count > 0 && depth < maxDepth)
            {
                depth++;
                var nextLevel = new List<INode>();

                // Process all nodes at current level
                foreach (var currentNode in currentLevel)
                {
                    // Query outgoing edges matching pattern
                    var edgeResponse = await QueryEdgesForNodeAsync(
                        currentNode,
                        pattern.EdgePattern,
                        consistentRead,
                        cancellationToken);

                    // Process each connected node
                    foreach (var edgeKey in edgeResponse.Connection.Edges.Select(e => e.Node))
                    {
                        var targetNodeId = edgeKey.InId; // The "to" node (InId is the target in OutToEdge query)

                        if (visited.Add(targetNodeId))
                        {
                            // Fetch the target node
                            var targetNode = await GetNodeByIdAsync(targetNodeId, consistentRead, cancellationToken);

                            if (targetNode != null)
                            {
                                nextLevel.Add(targetNode);

                                // Add binding if we're at or above min depth
                                if (depth >= pattern.MinDepth)
                                {
                                    var binding = ImmutableDictionary<string, IEntity>.Empty
                                        .Add(pattern.EndVariable.Name, targetNode);
                                    resultBindings = resultBindings.Add(binding);
                                }
                            }
                        }
                    }
                }

                currentLevel = nextLevel;
            }

            return resultBindings;
        }

        /// <summary>
        /// Depth-first search for recursive pattern execution
        /// </summary>
        private async Task<BindingSet> DepthFirstSearchAsync(
            RecursivePattern pattern,
            BindingSet startBindings,
            int maxDepth,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var resultBindings = BindingSet.Empty;
            var visited = new HashSet<string>();

            foreach (var startBinding in startBindings.GetBindings(pattern.StartVariable.Name))
            {
                var startNode = (INode)startBinding.Value;
                var pathBindings = await DFSRecursiveAsync(
                    pattern,
                    startNode,
                    0,
                    maxDepth,
                    visited,
                    consistentRead,
                    cancellationToken);

                resultBindings = resultBindings.Union(pathBindings);
            }

            return resultBindings;
        }

        /// <summary>
        /// Recursive DFS helper
        /// </summary>
        private async Task<BindingSet> DFSRecursiveAsync(
            RecursivePattern pattern,
            INode currentNode,
            int currentDepth,
            int maxDepth,
            HashSet<string> visited,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            if (currentDepth >= maxDepth || !visited.Add(currentNode.Id))
            {
                return BindingSet.Empty;
            }

            var resultBindings = BindingSet.Empty;

            // Query edges
            var edgeResponse = await QueryEdgesForNodeAsync(
                currentNode,
                pattern.EdgePattern,
                consistentRead,
                cancellationToken);

            // Process each connected node
            foreach (var edgeKey in edgeResponse.Connection.Edges.Select(e => e.Node))
            {
                var targetNodeId = edgeKey.InId;
                var targetNode = await GetNodeByIdAsync(targetNodeId, consistentRead, cancellationToken);

                if (targetNode != null)
                {
                    // Add to results if at or above min depth
                    if (currentDepth + 1 >= pattern.MinDepth)
                    {
                        var binding = ImmutableDictionary<string, IEntity>.Empty
                            .Add(pattern.EndVariable.Name, targetNode);
                        resultBindings = resultBindings.Add(binding);
                    }

                    // Recurse
                    var childBindings = await DFSRecursiveAsync(
                        pattern,
                        targetNode,
                        currentDepth + 1,
                        maxDepth,
                        visited,
                        consistentRead,
                        cancellationToken);

                    resultBindings = resultBindings.Union(childBindings);
                }
            }

            return resultBindings;
        }

        /// <summary>
        /// Queries edges for a given node matching the edge pattern
        /// </summary>
        private async Task<ToEdgeQueryResponse> QueryEdgesForNodeAsync(
            INode node,
            EdgePattern edgePattern,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            // Create initial cursor for node
            var initialCursor = Cursor.Create(CursorNode.Empty);
            var serializedCursor = _cursorSerializer.Serialize(initialCursor);

            // Create a connection with just this node
            var nodeConnection = new Connection<RelayEdge<INode>, INode>(
                ImmutableList.Create(new RelayEdge<INode>(serializedCursor, node)),
                new PageInfo(false, false, string.Empty, string.Empty));

            var request = new ToEdgeQueryRequest(
                NodeTypeName: node.GetType().Name,
                EdgeTypeName: edgePattern.EdgeTypeName,
                NodeConnection: nodeConnection,
                OrderBy: null,
                FilterBy: null,
                ConnectionArguments: ConnectionArguments.FirstMax,
                ConsistentRead: consistentRead);

            return await _graphQueryService.GetOutToEdgeConnectionAsync(request, cancellationToken);
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
    }
}
