/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.Query
{
    public static class GraphResultExtensions
    {
        public static bool IsConnection(this GraphResult source)
        {
            return source switch
            {
                NodeConnectionResult => true,
                EdgeConnectionResult => true,
                NodeResult => false,
                EdgeResult => false,
                _ => throw new NotSupportedException(),
            };
        }

        public static Connection<RelayEdge<T>, T> GetConnection<T>(this GraphResult source)
            where T : INode
        {
            return source switch
            {
                NodeConnectionResult result => result.Connection.AsType<T>(),
                NodeResult result => result.Node?.AsType<T>().AsConnection() ?? Connection<RelayEdge<T>, T>.Empty,
                _ => throw new NotSupportedException(),
            };
        }

        public static Connection<RelayEdge<T>, T> GetEdgeConnection<T>(this GraphResult source)
            where T : IEdge
        {
            return source switch
            {
                EdgeConnectionResult result => result.Connection.AsType<T>(),
                EdgeResult result => result.Edge?.AsType<T>().AsConnection() ?? Connection<RelayEdge<T>, T>.Empty,
                _ => throw new NotSupportedException(),
            };
        }

        public static PageInfo GetPageInfo(this GraphResult source)
        {
            return source switch
            {
                NodeConnectionResult result => result.Connection.PageInfo,
                EdgeConnectionResult result => result.Connection.PageInfo,
                _ => throw new NotSupportedException(),
            };
        }

        public static RelayEdge<T>? TryGetRelayEdge<T>(this GraphResult source)
        where T : INode
        {
            return source switch
            {
                NodeResult result => result.Node?.AsType<T>(),
                _ => throw new NotSupportedException(),
            };
        }


        public static RelayEdge<T>? TryGetRelayEdgeEdge<T>(this GraphResult source)
        where T : IEdge
        {
            return source switch
            {
                EdgeResult result => result.Edge?.AsType<T>(),
                _ => throw new NotSupportedException(),
            };
        }

        public static RelayEdge<T> GetRelayEdge<T>(this GraphResult source)
        where T : INode
        {
            return source switch
            {
                NodeResult result => result.Node?.AsType<T>() ?? throw new GraphlessDBOperationException("Expected node of specified type"),
                _ => throw new NotSupportedException(),
            };
        }
        public static RelayEdge<T> GetRelayEdgeEdge<T>(this GraphResult source)
        where T : IEdge
        {
            return source switch
            {
                EdgeResult result => result.Edge?.AsType<T>() ?? throw new GraphlessDBOperationException("Expected edge of specified type"),
                _ => throw new NotSupportedException(),
            };
        }
    }
}
