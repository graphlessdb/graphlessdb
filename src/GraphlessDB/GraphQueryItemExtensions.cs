/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Query;

namespace GraphlessDB
{
    public static class GraphQueryItemExtensions
    {
        public static GraphQueryNode WithCursor(this GraphQueryNode source, string value)
        {
            return source.Query switch
            {
                NodeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCursor(value) }),
                InToEdgeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCursor(value) }),
                InToAllEdgeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCursor(value) }),
                OutToEdgeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCursor(value) }),
                OutToAllEdgeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCursor(value) }),
                InAndOutToEdgeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCursor(value) }),
                ZipNodeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCursor(value) }),
                WhereNodeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCursor(value) }),
                WhereEdgeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCursor(value) }),
                _ => throw new NotSupportedException($"Type '{source.Query.GetType().Name}' is not supported"),
            };
        }

        public static bool SupportsConnectionArguments(this GraphQueryNode source)
        {
            return source.Query switch
            {
                NodeConnectionQuery => true,
                InToEdgeConnectionQuery => true,
                InToAllEdgeConnectionQuery => true,
                OutToEdgeConnectionQuery => true,
                OutToAllEdgeConnectionQuery => true,
                InAndOutToEdgeConnectionQuery => true,
                ZipNodeConnectionQuery => true,
                WhereNodeConnectionQuery => true,
                WhereEdgeConnectionQuery => true,
                _ => false,
            };
        }

        public static ConnectionArguments GetConnectionArguments(this GraphQueryNode source)
        {
            return source.Query switch
            {
                NodeConnectionQuery query => query.Page,
                InToEdgeConnectionQuery query => query.Page,
                InToAllEdgeConnectionQuery query => query.Page,
                OutToEdgeConnectionQuery query => query.Page,
                OutToAllEdgeConnectionQuery query => query.Page,
                InAndOutToEdgeConnectionQuery query => query.Page,
                ZipNodeConnectionQuery query => query.Page,
                WhereNodeConnectionQuery query => query.Page,
                WhereEdgeConnectionQuery query => query.Page,
                _ => throw new NotSupportedException($"Type '{source.Query.GetType().Name}' is not supported"),
            };
        }

        public static GraphQueryNode WithConnectionSize(this GraphQueryNode source, int pageSize)
        {
            return source.Query switch
            {
                NodeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCount(pageSize) }),
                InToEdgeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCount(pageSize) }),
                InToAllEdgeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCount(pageSize) }),
                OutToEdgeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCount(pageSize) }),
                OutToAllEdgeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCount(pageSize) }),
                InAndOutToEdgeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCount(pageSize) }),
                ZipNodeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCount(pageSize) }),
                WhereNodeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCount(pageSize) }),
                WhereEdgeConnectionQuery query => new GraphQueryNode(query with { Page = query.Page.WithCount(pageSize) }),
                _ => throw new NotSupportedException($"Type '{source.Query.GetType().Name}' is not supported"),
            };
        }

        public static GraphQueryNode WithPreFilteredConnectionSize(this GraphQueryNode source, int pageSize)
        {
            return source.Query switch
            {
                NodeConnectionQuery query => new GraphQueryNode(query with { PreFilteredPageSize = pageSize }),
                InToEdgeConnectionQuery query => new GraphQueryNode(query with { PreFilteredPageSize = pageSize }),
                InToAllEdgeConnectionQuery query => new GraphQueryNode(query with { PreFilteredPageSize = pageSize }),
                OutToEdgeConnectionQuery query => new GraphQueryNode(query with { PreFilteredPageSize = pageSize }),
                OutToAllEdgeConnectionQuery query => new GraphQueryNode(query with { PreFilteredPageSize = pageSize }),
                InAndOutToEdgeConnectionQuery query => new GraphQueryNode(query with { PreFilteredPageSize = pageSize }),
                ZipNodeConnectionQuery query => new GraphQueryNode(query with { PreFilteredPageSize = pageSize }),
                WhereNodeConnectionQuery query => new GraphQueryNode(query with { PreFilteredPageSize = pageSize }),
                WhereEdgeConnectionQuery query => new GraphQueryNode(query with { PreFilteredPageSize = pageSize }),
                _ => throw new NotSupportedException($"Type '{source.Query.GetType().Name}' is not supported"),
            };
        }

        public static GraphQueryNode WithConnectionArguments(this GraphQueryNode source, ConnectionArguments page)
        {
            return source.Query switch
            {
                NodeConnectionQuery query => new GraphQueryNode(query with { Page = page }),
                InToEdgeConnectionQuery query => new GraphQueryNode(query with { Page = page }),
                InToAllEdgeConnectionQuery query => new GraphQueryNode(query with { Page = page }),
                OutToEdgeConnectionQuery query => new GraphQueryNode(query with { Page = page }),
                OutToAllEdgeConnectionQuery query => new GraphQueryNode(query with { Page = page }),
                InAndOutToEdgeConnectionQuery query => new GraphQueryNode(query with { Page = page }),
                ZipNodeConnectionQuery query => new GraphQueryNode(query with { Page = page }),
                WhereNodeConnectionQuery query => new GraphQueryNode(query with { Page = page }),
                WhereEdgeConnectionQuery query => new GraphQueryNode(query with { Page = page }),
                _ => throw new NotSupportedException($"Type '{source.Query.GetType().Name}' is not supported"),
            };
        }

        public static GraphQueryNode WithConsistentRead(this GraphQueryNode source, bool consistentRead)
        {
            return source.Query switch
            {
                FirstNodeQuery => source,
                FirstOrDefaultNodeQuery => source,
                FromEdgeConnectionQuery query => new GraphQueryNode(query with { ConsistentRead = consistentRead }),
                FromEdgeQuery query => new GraphQueryNode(query with { ConsistentRead = consistentRead }),
                ToEdgeConnectionQuery query => new GraphQueryNode(query with { ConsistentRead = consistentRead }),
                NodeByIdQuery query => new GraphQueryNode(query with { ConsistentRead = consistentRead }),
                NodeByNodeQuery query => new GraphQueryNode(query with { ConsistentRead = consistentRead }),
                NodeOrDefaultByIdQuery query => new GraphQueryNode(query with { ConsistentRead = consistentRead }),
                NodeVersionByIdQuery query => new GraphQueryNode(query with { ConsistentRead = consistentRead }),
                SingleEdgeQuery => source,
                SingleNodeQuery => source,
                SingleOrDefaultEdgeQuery => source,
                SingleOrDefaultNodeQuery => source,
                FirstEdgeQuery => source,
                FirstOrDefaultEdgeQuery => source,
                ZipNodeConnectionQuery => source,
                WhereNodeConnectionQuery => source,
                WhereEdgeConnectionQuery => source,
                EdgeByIdQuery query => new GraphQueryNode(query with { ConsistentRead = consistentRead }),
                EdgeOrDefaultByIdQuery query => new GraphQueryNode(query with { ConsistentRead = consistentRead }),
                NodeConnectionQuery query => new GraphQueryNode(query with { ConsistentRead = consistentRead }),
                _ => throw new NotSupportedException($"Type '{source.Query.GetType().Name}' is not supported"),
            };
        }
    }
}
