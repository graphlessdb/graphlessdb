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

namespace GraphlessDB
{
    public static class ConnectionExtensions
    {
        public static Connection<RelayEdge<T>, T> AsType<T>(this Connection<RelayEdge<INode>, INode> connection)
            where T : INode
        {
            return new Connection<RelayEdge<T>, T>(
                connection.Edges.Select(e => e.AsType<T>()).ToImmutableList(),
                connection.PageInfo);
        }

        public static Connection<RelayEdge<T>, T> AsType<T>(this Connection<RelayEdge<IEdge>, IEdge> connection)
            where T : IEdge
        {
            return new Connection<RelayEdge<T>, T>(
                connection.Edges.Select(e => e.AsType<T>()).ToImmutableList(),
                connection.PageInfo);
        }

        public static Connection<RelayEdge<T>, T> FromCursorInclusive<T>(this Connection<RelayEdge<T>, T> source, string cursor)
        {
            if (source.Edges.Where(e => e.Cursor == cursor).Count() > 1)
            {
                throw new GraphlessDBOperationException("Should not be duplicate cursors");
            }

            var index = source.Edges.FindIndex(e => e.Cursor == cursor);
            if (index < 0)
            {
                throw new GraphlessDBOperationException("Matching cursor not found");
            }

            var edges = source.Edges.Skip(index).ToImmutableList();
            return new Connection<RelayEdge<T>, T>(
                edges,
                new PageInfo(false, false, edges.TryGetStartCursor() ?? string.Empty, edges.TryGetEndCursor() ?? string.Empty));
        }

        public static Connection<RelayEdge<T>, T> FromCursorExclusive<T>(this Connection<RelayEdge<T>, T> source, string cursor)
        {
            if (source.Edges.Where(e => e.Cursor == cursor).Count() > 1)
            {
                throw new GraphlessDBOperationException("Should not be duplicate cursors");
            }

            var index = source.Edges.FindIndex(e => e.Cursor == cursor);
            if (index < 0)
            {
                throw new GraphlessDBOperationException("Matching cursor not found");
            }

            var edges = source.Edges.Skip(index + 1).ToImmutableList();
            return new Connection<RelayEdge<T>, T>(
                edges,
                new PageInfo(false, false, edges.TryGetStartCursor() ?? string.Empty, edges.TryGetEndCursor() ?? string.Empty));
        }

        public static Connection<RelayEdge<T>, T> ToCursorInclusive<T>(this Connection<RelayEdge<T>, T> source, string cursor)
        {
            if (source.Edges.Where(e => e.Cursor == cursor).Count() > 1)
            {
                throw new GraphlessDBOperationException("Should not be duplicate cursors");
            }

            var index = source.Edges.FindIndex(e => e.Cursor == cursor);
            if (index < 0)
            {
                throw new GraphlessDBOperationException("Matching cursor not found");
            }

            var edges = source.Edges.Take(index + 1).ToImmutableList();
            return new Connection<RelayEdge<T>, T>(
                edges,
                new PageInfo(false, false, edges.TryGetStartCursor() ?? string.Empty, edges.TryGetEndCursor() ?? string.Empty));
        }

        public static Connection<RelayEdge<T>, T> ToCursorExclusive<T>(this Connection<RelayEdge<T>, T> source, string cursor)
        {
            if (source.Edges.Where(e => e.Cursor == cursor).Count() > 1)
            {
                throw new GraphlessDBOperationException("Should not be duplicate cursors");
            }

            var index = source.Edges.FindIndex(e => e.Cursor == cursor);
            if (index < 0)
            {
                throw new GraphlessDBOperationException("Matching cursor not found");
            }

            var edges = source.Edges.Take(index).ToImmutableList();
            return new Connection<RelayEdge<T>, T>(
                edges,
                new PageInfo(false, false, edges.TryGetStartCursor() ?? string.Empty, edges.TryGetEndCursor() ?? string.Empty));
        }

#pragma warning disable CA1720
        public static Connection<TEdge, TNodeOut> Single<TEdge, TNodeOut>(this Connection<TEdge, TNodeOut> source)
            where TEdge : IRelayEdge<TNodeOut>
            where TNodeOut : class
        {
            return new Connection<TEdge, TNodeOut>([source.Edges.Single()], source.PageInfo);
        }
#pragma warning restore CA1720

        public static Connection<TEdge, TNodeOut> SingleOrDefault<TEdge, TNodeOut>(this Connection<TEdge, TNodeOut> source)
            where TEdge : IRelayEdge<TNodeOut>
            where TNodeOut : class
        {
            var edges = ImmutableList<TEdge>.Empty;
            var edge = source.Edges.SingleOrDefault();
            if (edge != null)
            {
                edges = edges.Add(edge);
            }

            return new Connection<TEdge, TNodeOut>(edges, source.PageInfo);
        }

        public static Connection<RelayEdge<TNodeOut>, TNodeOut> Select<TNodeIn, TNodeOut>(
            this Connection<RelayEdge<TNodeIn>, TNodeIn> source, Func<RelayEdge<TNodeIn>, int, RelayEdge<TNodeOut>> selector)
            where TNodeIn : class
            where TNodeOut : class
        {
            return new Connection<RelayEdge<TNodeOut>, TNodeOut>(
                source.Edges.Select(selector).ToImmutableList(),
                source.PageInfo);
        }

        public static Connection<RelayEdge<TNodeOut>, TNodeOut> Select<TNodeIn, TNodeOut>(
            this Connection<RelayEdge<TNodeIn>, TNodeIn> source, Func<RelayEdge<TNodeIn>, RelayEdge<TNodeOut>> selector)
            where TNodeIn : class
            where TNodeOut : class
        {
            return new Connection<RelayEdge<TNodeOut>, TNodeOut>(
                source.Edges.Select(selector).ToImmutableList(),
                source.PageInfo);
        }

        public static Connection<TEdge, TNodeOut> First<TEdge, TNodeOut>(this Connection<TEdge, TNodeOut> source)
            where TEdge : IRelayEdge<TNodeOut>
            where TNodeOut : class
        {
            return new Connection<TEdge, TNodeOut>(
                [source.Edges.First()],
                source.PageInfo);
        }

        public static Connection<TEdge, TNodeOut> FirstOrDefault<TEdge, TNodeOut>(this Connection<TEdge, TNodeOut> source)
            where TEdge : IRelayEdge<TNodeOut>
            where TNodeOut : class
        {
            var edge = source.Edges.FirstOrDefault();
            var edges = edge != null ? [edge] : ImmutableList<TEdge>.Empty;
            return new Connection<TEdge, TNodeOut>(edges, source.PageInfo);
        }

        public static Connection<TEdge, TNode> Truncate<TEdge, TNode>(this Connection<TEdge, TNode> source, ConnectionArguments page)
            where TEdge : IRelayEdge<TNode>
            where TNode : class
        {
            if (source.Edges.Count <= page.Count())
            {
                return source;
            }

            var pagedEdges = source
                .Edges
                .Take(page.Count())
                .ToImmutableList();

            var hasNextPage = !string.IsNullOrWhiteSpace(page.After) || source.PageInfo.HasNextPage;
            var hasPreviousPage = !string.IsNullOrWhiteSpace(page.Before) || source.PageInfo.HasPreviousPage;
            var startCursor = pagedEdges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty;
            var endCursor = pagedEdges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty;
            return new Connection<TEdge, TNode>(
                pagedEdges,
                new PageInfo(hasNextPage, hasPreviousPage, startCursor, endCursor));
        }

        public static Connection<TEdge, TNode> GetPagedConnection<TEdge, TNode>(this Connection<TEdge, TNode> source, ConnectionArguments? page)
            where TEdge : IRelayEdge<TNode>
            where TNode : class
        {
            if (page == null || (string.IsNullOrWhiteSpace(page.After) && string.IsNullOrWhiteSpace(page.Before)))
            {
                return source;
            }

            if (!string.IsNullOrWhiteSpace(page.Before))
            {
                throw new NotSupportedException();
            }

            var pagedEdges = source
                .Edges
                .Where(e => string.IsNullOrWhiteSpace(page.After) || string.CompareOrdinal(e.Cursor, page.After) >= 0)
                .ToImmutableList();

            return new Connection<TEdge, TNode>(
                pagedEdges,
                source.PageInfo with
                {
                    StartCursor = pagedEdges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                    EndCursor = pagedEdges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty
                });
        }

        public static Connection<RelayEdge<T>, T> ToConnection<T>(this IEnumerable<RelayEdge<T>> source)
        {
            var edges = source.ToImmutableList();

            var pageInfo = new PageInfo(
                    false,
                    false,
                    edges.Select(e => e.Cursor).FirstOrDefault() ?? string.Empty,
                    edges.Select(e => e.Cursor).LastOrDefault() ?? string.Empty);

            return new Connection<RelayEdge<T>, T>(edges, pageInfo);
        }

        public static ImmutableList<TNode> ToImmutableNodeList<TNode>(
            this Connection<RelayEdge<TNode>, TNode> source)
        // where TNode : INode
        {
            return source.Edges.Select(e => e.Node).ToImmutableList();
        }
    }
}