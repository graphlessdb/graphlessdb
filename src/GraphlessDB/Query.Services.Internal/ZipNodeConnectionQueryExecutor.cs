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
using GraphlessDB;
using GraphlessDB.Collections;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services.Internal;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class ZipNodeConnectionQueryExecutor(IGraphCursorSerializationService cursorSerializer) : IGraphQueryNodeExecutionService<ZipNodeConnectionQuery>
    {
        public async Task<GraphExecutionContext> ExecuteAsync(
            GraphExecutionContext context,
            string key,
            CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            var query = context.GetQuery<ZipNodeConnectionQuery>(key);
            // var result = context.TryGetResult<NodeConnectionResult>(key);
            // if (CanRestoreIntermediateResultFromCursor(query, result, context.Query.IsRootConnectionArgumentsKey(key)))
            // {
            //     result = await RestoreIntermediateResultFromCursorAsync(query, cancellationToken);
            // }

            var childResults = context
                .GetChildResults(key)
                .Cast<NodeConnectionResult>()
                .ToImmutableList();

            var result = ExecuteIteration(query, childResults);

            if (result != null)
            {
                context = context.SetResult(key, result);
            }

            return context;
        }

        private NodeConnectionResult ExecuteIteration(
            ZipNodeConnectionQuery query,
            ImmutableList<NodeConnectionResult> childResults)
        {
            var pageCursorString = query.Page.CursorOrDefault();

            var pageCursor = pageCursorString != null
                ? cursorSerializer.Deserialize(pageCursorString)
                : null;

            if (childResults.Count != 2)
            {
                throw new NotSupportedException();
            }

            var childConnections = childResults.Select(r => r.Connection).ToImmutableList();

            var allEdges = GetEdges(query, pageCursor, childConnections);

            // NOTE : A nasty hack to remove any 'close' duplicates
            // found from the various child sources 
            allEdges = allEdges
                .Reverse()
                .DistinctBy(v => v.Node.Id)
                .Reverse()
                .ToImmutableList();

            var edges = allEdges
                .Take(query.Page.Count())
                .ToImmutableList();

            var hasNextPage = allEdges.Count > edges.Count || childResults.Select(r => r.Connection.PageInfo).Where(p => p.HasNextPage).Any();

            // TODO
            var hasPreviousPage = childResults.Select(r => r.Connection.PageInfo).Where(p => p.HasPreviousPage).Any();

            var pageInfo = new PageInfo(
                hasNextPage,
                hasPreviousPage,
                edges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                edges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty);

            var childCursor = allEdges.TryGetEndCursor();
            var cursor = pageInfo.GetNullableEndCursor() ?? cursorSerializer.Serialize(Cursor.Create(CursorNode.CreateEndOfData()));
            return new NodeConnectionResult(
                childCursor,
                cursor,
                GetNeedsMoreData(query, childResults, childConnections, allEdges),
                false,
                new Connection<RelayEdge<INode>, INode>(edges, pageInfo))
                .EnsureValid();
        }

        private ImmutableList<RelayEdge<INode>> GetEdges(
            ZipNodeConnectionQuery query,
            Cursor? pageCursor,
            ImmutableList<Connection<RelayEdge<INode>, INode>> childConnections)
        {
            var indexedCursor = pageCursor?.GetRootNode().Indexed;
            var startCursor = new Cursor(ImmutableTree<string, CursorNode>.Empty);
            var endCursor = Cursor.Create(CursorNode.CreateEndOfData());
            var childCursors = pageCursor != null
                ? pageCursor.GetSubTrees(pageCursor.GetRootKey())
                : childConnections.Select(c => c.Edges.Count == 0 ? endCursor : startCursor).ToImmutableList();

            var builder = ImmutableList.CreateBuilder<RelayEdge<INode>>();
            var balancedCount = childConnections.Where(r => r.PageInfo.HasNextPage).Any()
                ? childConnections.Where(r => r.PageInfo.HasNextPage).Min(r => r.Edges.Count)
                : (int)Math.Ceiling(query.Page.Count() / (double)childConnections.Count);

            var maxCount = childConnections.Max(r => r.Edges.Count);
            var currentChildCursors = childCursors;
            for (var i = 0; i < maxCount; i++)
            {
                var childNodes = childConnections
                    .Select(r => r.Edges.Count > i ? r.Edges[i] : null)
                    .ToImmutableList();

                if (i >= balancedCount && childNodes.Where((n, index) => n == null && childConnections[index].PageInfo.HasNextPage).Any())
                {
                    // NOTE: We need to collect a balanced set of nodes.  If the counts are not equivalent across the child
                    // connections then we should stop.  This excludes child connections that have genuinely run out of data
                    break;
                }

                var newChildCursors = childConnections
                    .Select((r, index) => r.Edges.Count > i ? cursorSerializer.Deserialize(r.Edges[i].Cursor) : currentChildCursors[index])
                    .ToImmutableList();

                builder.AddRange(childNodes
                    .Select((c, index) => new { c, index })
                    .Where(v => v.c?.Node != null && !ShouldSkip(i, v.index, indexedCursor))
                    .Select(v => GetRelayNode(v.c?.Node ?? throw new GraphlessDBOperationException("Expected node"), v.index, newChildCursors)));

                currentChildCursors = newChildCursors;
            }

            var allEdges = builder.ToImmutable();
            return allEdges;
        }

        private static bool ShouldSkip(int zipIndex, int subZipIndex, IndexedCursor? cursor)
        {
            // Can only skip on the first zipped set of nodes and where cursor has been provided
            if (zipIndex > 0 || cursor == null)
            {
                return false;
            }

            return subZipIndex <= cursor.Index;
        }

#pragma warning disable IDE0060 // Remove unused parameter
        private static bool GetNeedsMoreData(
            ZipNodeConnectionQuery query,
            ImmutableList<NodeConnectionResult> childResults,
            ImmutableList<Connection<RelayEdge<INode>, INode>> childConnections,
            ImmutableList<RelayEdge<INode>> allEdges)
        {
            if (query.Page.Count() > allEdges.Count)
            {
                return true;
            }

            // var targetCount = childConnections.Max(r => r.Edges.Count);
            // if (childConnections.Select((c, i) => { return c.Edges.Count == targetCount || !c.PageInfo.HasNextPage; }).Any())
            // {
            //     return true;
            // }

            return false;
        }
#pragma warning restore IDE0060 // Remove unused parameter

        public bool HasMoreChildData(
            GraphExecutionContext context,
            string key)
        {
            var result = context.GetResult<NodeConnectionResult>(key);
            var childResults = context.GetChildResults(key).Cast<NodeConnectionResult>().ToImmutableList();
            var childCursors = GetCursors(childResults.Count, result.ChildCursor);
            return childResults
                .Select((r, i) =>
                {
                    return
                        !r.Connection.Edges.IsEmpty &&
                        (childCursors[i] != r.Connection.PageInfo.GetNullableEndCursor() || r.Connection.PageInfo.HasNextPage);
                })
                .All(v => v);
        }

        private ImmutableList<string> GetCursors(int count, string? value)
        {
            if (value == null)
            {
                return Enumerable
                    .Range(0, count)
                    .Select(i => string.Empty)
                    .ToImmutableList();
            }

            var cursor = cursorSerializer.Deserialize(value);
            return cursor
                .GetSubTrees(cursor.GetRootKey())
                .Select(cursorSerializer.Serialize)
                .ToImmutableList();
        }

        private RelayEdge<INode> GetRelayNode(INode node, int index, ImmutableList<Cursor> childCursors)
        {
            var cursor = childCursors.Aggregate(
                Cursor.Create(CursorNode.Empty with { Indexed = new IndexedCursor(index) }),
                (agg, cur) => cur.GetNodeCount() > 0 ? agg.AddSubTree(cur, agg.GetRootKey()) : agg);

            return new RelayEdge<INode>(
                cursorSerializer.Serialize(cursor),
                node);
        }
    }
}
