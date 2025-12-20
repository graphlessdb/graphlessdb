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
using GraphlessDB.Query.Datalog.Core;

namespace GraphlessDB.Query.Datalog.Execution
{
    /// <summary>
    /// Compiles datalog patterns into an optimized execution plan
    /// </summary>
    internal sealed class DatalogPatternCompiler
    {
        /// <summary>
        /// Compiles a datalog query into an optimized execution plan
        /// </summary>
        public ExecutionPlan CompilePlan(DatalogQuery query)
        {
            // Build dependency graph
            var graph = BuildDependencyGraph(query.Patterns);

            // Optimize join order using cost-based optimization
            var orderedPatterns = OptimizeJoinOrder(query.Patterns, graph);

            // Compile patterns into executable form
            var compiledPatterns = orderedPatterns
                .Select(CompilePattern)
                .ToImmutableList();

            return new ExecutionPlan(compiledPatterns);
        }

        /// <summary>
        /// Builds a dependency graph showing which variables are bound by which patterns
        /// </summary>
        private static DependencyGraph BuildDependencyGraph(ImmutableList<Pattern> patterns)
        {
            var graph = new DependencyGraph();

            foreach (var pattern in patterns)
            {
                switch (pattern)
                {
                    case NodePattern np:
                        graph.AddProducer(pattern.Id, new[] { np.Variable.Name });
                        break;

                    case EdgePattern ep:
                        graph.AddProducer(pattern.Id, new[] { ep.FromVariable.Name, ep.ToVariable.Name });
                        graph.AddConsumer(pattern.Id, ep.FromVariable.Name);
                        graph.AddConsumer(pattern.Id, ep.ToVariable.Name);
                        break;

                    case JoinPattern jp:
                        graph.AddConsumer(pattern.Id, jp.LeftVariable.Name);
                        graph.AddConsumer(pattern.Id, jp.RightVariable.Name);
                        break;
                }
            }

            return graph;
        }

        /// <summary>
        /// Optimizes join order using a greedy heuristic
        /// Prioritizes patterns with selective filters and bound variables
        /// </summary>
        private static ImmutableList<Pattern> OptimizeJoinOrder(
            ImmutableList<Pattern> patterns,
            DependencyGraph graph)
        {
            var ordered = new List<Pattern>();
            var remaining = patterns.ToList();
            var boundVariables = new HashSet<string>();

            while (remaining.Count > 0)
            {
                // Find the best pattern to execute next
                Pattern? bestPattern = null;
                int bestScore = int.MinValue;

                foreach (var pattern in remaining)
                {
                    int score = CalculatePatternScore(pattern, boundVariables);

                    if (score > bestScore)
                    {
                        bestScore = score;
                        bestPattern = pattern;
                    }
                }

                if (bestPattern == null)
                    break;

                // Add to ordered list and remove from remaining
                ordered.Add(bestPattern);
                remaining.Remove(bestPattern);

                // Update bound variables
                var producedVars = graph.GetProducedVariables(bestPattern.Id);
                foreach (var v in producedVars)
                {
                    boundVariables.Add(v);
                }
            }

            return ordered.ToImmutableList();
        }

        /// <summary>
        /// Calculates a score for pattern execution priority
        /// Higher scores indicate better patterns to execute first
        /// </summary>
        private static int CalculatePatternScore(Pattern pattern, HashSet<string> boundVariables)
        {
            int score = 0;

            switch (pattern)
            {
                case NodePattern np:
                    // Prioritize patterns with filters (more selective)
                    score += np.PropertyConstraints.Count * 100;

                    // Prioritize patterns that bind new variables
                    if (!boundVariables.Contains(np.Variable.Name))
                    {
                        score += 50;
                    }
                    break;

                case EdgePattern ep:
                    // Heavily prioritize edge patterns where one side is already bound
                    var fromBound = boundVariables.Contains(ep.FromVariable.Name);
                    var toBound = boundVariables.Contains(ep.ToVariable.Name);

                    if (fromBound && toBound)
                    {
                        // Both bound - very cheap to execute (just verification)
                        score += 200;
                    }
                    else if (fromBound || toBound)
                    {
                        // One bound - cheap edge traversal
                        score += 150;
                    }
                    else
                    {
                        // Neither bound - expensive cross product, do last
                        score -= 100;
                    }

                    // Add bonus for edge property constraints
                    score += ep.EdgePropertyConstraints.Count * 50;
                    break;

                case JoinPattern jp:
                    // Joins should execute after both variables are bound
                    var leftBound = boundVariables.Contains(jp.LeftVariable.Name);
                    var rightBound = boundVariables.Contains(jp.RightVariable.Name);

                    if (leftBound && rightBound)
                    {
                        score += 100;
                    }
                    else
                    {
                        // Can't execute join if variables aren't bound
                        score = int.MinValue;
                    }
                    break;
            }

            return score;
        }

        /// <summary>
        /// Compiles a pattern into its executable representation
        /// </summary>
        private CompiledPattern CompilePattern(Pattern pattern)
        {
            return pattern switch
            {
                NodePattern np => CompileNodePattern(np),
                EdgePattern ep => CompileEdgePattern(ep),
                JoinPattern jp => CompileJoinPattern(jp),
                _ => throw new NotSupportedException($"Unknown pattern type: {pattern.GetType()}")
            };
        }

        /// <summary>
        /// Compiles a node pattern with filter and pushdown analysis
        /// </summary>
        private static CompiledNodePattern CompileNodePattern(NodePattern pattern)
        {
            // Convert property constraints into NodeFilter
            var filter = CreateNodeFilter(pattern.PropertyConstraints);

            // Analyze what can be pushed down to the data layer
            var pushdownData = AnalyzePushdown(pattern.PropertyConstraints);

            return new CompiledNodePattern(
                pattern.Id,
                pattern.Variable.Name,
                pattern.NodeTypeName,
                filter,
                pushdownData);
        }

        /// <summary>
        /// Creates a NodeFilter from property constraints
        /// </summary>
        private static NodeFilter? CreateNodeFilter(ImmutableList<PropertyConstraint> constraints)
        {
            if (constraints.IsEmpty)
                return null;

            var valueFilters = constraints
                .Select(c => new ValueFilterItem(c.PropertyName, c.Filter))
                .ToImmutableList();

            return new NodeFilter(valueFilters, ImmutableList<EdgeFilter>.Empty, null);
        }

        /// <summary>
        /// Analyzes which filters can be pushed down to the data layer
        /// Currently supports Equals and StartsWith on single properties
        /// </summary>
        private static NodePushdownData? AnalyzePushdown(ImmutableList<PropertyConstraint> constraints)
        {
            // For now, we'll look for simple single-property Equals or StartsWith filters
            // More sophisticated analysis can be added later

            foreach (var constraint in constraints)
            {
                if (constraint.Filter is StringFilter sf)
                {
                    if (sf.Eq != null)
                    {
                        return new NodePushdownData(
                            constraint.PropertyName,
                            PropertyOperator.Equals,
                            ImmutableList.Create(sf.Eq));
                    }
                    else if (sf.BeginsWith != null)
                    {
                        return new NodePushdownData(
                            constraint.PropertyName,
                            PropertyOperator.StartsWith,
                            ImmutableList.Create(sf.BeginsWith));
                    }
                    else if (sf.BeginsWithAny != null && sf.BeginsWithAny.Length > 0)
                    {
                        // Use the first BeginsWith value for now
                        return new NodePushdownData(
                            constraint.PropertyName,
                            PropertyOperator.StartsWith,
                            ImmutableList.Create(sf.BeginsWithAny.First()));
                    }
                    else if (sf.In != null && sf.In.Length > 0)
                    {
                        // Multiple equals - can push down
                        return new NodePushdownData(
                            constraint.PropertyName,
                            PropertyOperator.Equals,
                            sf.In.ToImmutableList());
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Compiles an edge pattern
        /// </summary>
        private static CompiledEdgePattern CompileEdgePattern(EdgePattern pattern)
        {
            // Convert edge constraints into EdgeFilter
            var filter = CreateEdgeFilter(pattern);

            return new CompiledEdgePattern(
                pattern.Id,
                pattern.FromVariable.Name,
                pattern.ToVariable.Name,
                pattern.EdgeTypeName,
                filter);
        }

        /// <summary>
        /// Creates an EdgeFilter from edge property constraints
        /// </summary>
        private static EdgeFilter? CreateEdgeFilter(EdgePattern pattern)
        {
            if (pattern.EdgePropertyConstraints.IsEmpty)
                return null;

            var valueFilters = pattern.EdgePropertyConstraints
                .Select(c => new ValueFilterItem(c.PropertyName, c.Filter))
                .ToImmutableList();

            // We don't know the node type names at compile time for edges,
            // so we'll leave them empty for now. The executor will fill them in.
            return new EdgeFilter(
                pattern.EdgeTypeName,
                string.Empty, // Will be filled by executor
                string.Empty, // Will be filled by executor
                null,
                null,
                valueFilters);
        }

        /// <summary>
        /// Compiles a join pattern
        /// </summary>
        private static CompiledJoinPattern CompileJoinPattern(JoinPattern pattern)
        {
            return new CompiledJoinPattern(
                pattern.Id,
                pattern.LeftVariable.Name,
                pattern.RightVariable.Name,
                pattern.JoinType);
        }
    }

    /// <summary>
    /// Dependency graph for pattern ordering
    /// </summary>
    internal sealed class DependencyGraph
    {
        private readonly Dictionary<string, HashSet<string>> _producers = new();
        private readonly Dictionary<string, HashSet<string>> _consumers = new();

        public void AddProducer(string patternId, string[] variables)
        {
            if (!_producers.ContainsKey(patternId))
                _producers[patternId] = new HashSet<string>();

            foreach (var v in variables)
                _producers[patternId].Add(v);
        }

        public void AddConsumer(string patternId, string variable)
        {
            if (!_consumers.ContainsKey(patternId))
                _consumers[patternId] = new HashSet<string>();

            _consumers[patternId].Add(variable);
        }

        public IEnumerable<string> GetProducedVariables(string patternId)
        {
            return _producers.TryGetValue(patternId, out var vars)
                ? vars
                : Enumerable.Empty<string>();
        }

        public IEnumerable<string> GetConsumedVariables(string patternId)
        {
            return _consumers.TryGetValue(patternId, out var vars)
                ? vars
                : Enumerable.Empty<string>();
        }
    }
}
