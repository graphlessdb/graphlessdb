/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Generic;
using System.Linq;

namespace GraphlessDB
{
    public static class RelayEdgeExtensions
    {
        public static RelayEdge<T> AsType<T>(this RelayEdge<INode> source)
            where T : INode
        {
            return new RelayEdge<T>(source.Cursor, (T)source.Node);
        }

        public static RelayEdge<T> AsType<T>(this RelayEdge<IEdge> source)
            where T : IEdge
        {
            return new RelayEdge<T>(source.Cursor, (T)source.Node);
        }

        public static Connection<RelayEdge<T>, T> AsConnection<T>(this RelayEdge<T> source)
        {
            return new Connection<RelayEdge<T>, T>(
                [source],
                new PageInfo(false, false, source.Cursor, source.Cursor));
        }

        public static string? GetNullableCursor<T>(this RelayEdge<T> source)
        {
            return string.IsNullOrEmpty(source.Cursor)
                ? null
                : source.Cursor;
        }

        public static string? TryGetStartCursor<T>(this IEnumerable<RelayEdge<T>> source)
        {
            return source.Select(e => e.Cursor).FirstOrDefault();
        }

        public static string? TryGetEndCursor<T>(this IEnumerable<RelayEdge<T>> source)
        {
            return source.Select(e => e.Cursor).LastOrDefault();
        }
    }
}
