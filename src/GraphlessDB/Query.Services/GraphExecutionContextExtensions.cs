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
using GraphlessDB;
using GraphlessDB.Collections;
using GraphlessDB.Query.Services;

namespace GraphlessDB.Query.Services
{
    public static class GraphExecutionContextExtensions
    {
        public static T GetRootResult<T>(this GraphExecutionContext source)
            where T : GraphResult
        {
            return source.TryGetRootResult<T>() ?? throw new GraphlessDBOperationException("Expected root result");
        }

        public static T? TryGetRootResult<T>(this GraphExecutionContext source)
            where T : GraphResult
        {
            return source.TryGetResult<T>(source.Query.GetRootKey());
        }

        public static T? TryGetParentResult<T>(this GraphExecutionContext source, string key)
            where T : GraphResult
        {
            if (source.Query.TryGetParentKey(key, out var parentKey) && parentKey != null)
            {
                return source.GetResult<T>(parentKey);
            }

            return default;
        }

        public static T? TryGetResult<T>(this GraphExecutionContext source, string key)
            where T : GraphResult
        {
            if (source.ResultItems.TryGetValue(key, out var value))
            {
                return (T)value;
            }

            return default;
        }

        public static GraphResult GetParentResult<T>(this GraphExecutionContext source, string key)
            where T : GraphResult
        {
            var parentKey = source.Query.GetParentKey(key);
            return source.GetResult<T>(parentKey);
        }

        public static ImmutableList<GraphResult> GetChildResults(this GraphExecutionContext source, string key)
        {
            return source
                .Query
                .GetChildNodeKeys(key)
                .Select(n => source.ResultItems[n])
                .ToImmutableList();
        }

        public static ImmutableList<GraphResult?> TryGetChildResults(this GraphExecutionContext source, string key)
        {
            return source
                .Query
                .GetChildNodeKeys(key)
                .Select(source.TryGetResult<GraphResult>)
                .ToImmutableList();
        }

        public static T GetSingleChildResult<T>(this GraphExecutionContext source, string key)
            where T : GraphResult
        {
            return source
                .GetChildResults(key)
                .Cast<T>()
                .Single();
        }

        public static T? TryGetSingleChildResult<T>(this GraphExecutionContext source, string key)
            where T : GraphResult
        {
            return source
                .TryGetChildResults(key)
                .Cast<T>()
                .SingleOrDefault();
        }

        public static T GetResult<T>(this GraphExecutionContext source, string key)
            where T : GraphResult
        {
            return (T)source.ResultItems[key];
        }

        public static GraphExecutionContext SetResult(this GraphExecutionContext source, string key, GraphResult result)
        {
            return source with
            {
                ResultItems = source.ResultItems.SetItem(key, result)
            };
        }

        public static T GetQuery<T>(this GraphExecutionContext source, string key)
            where T : GraphQuery
        {
            return (T)source.Query.Nodes.ByKey[key].Query;
        }

        public static GraphQuery GetSingleChildQuery(this GraphExecutionContext source, string key)
        {
            return source.Query.GetSingleChildNode(key).Query;
        }

        public static bool TryFindResult(this GraphExecutionContext source, Func<string, bool> predicate, out string resultKey)
        {
            var startKey = source.Query.GetRootKey();
            return TryFindResult(source, startKey, predicate, out resultKey);
        }

        // TODO Change to BFS to maintain balanced results
        private static bool TryFindResult(GraphExecutionContext source, string key, Func<string, bool> predicate, out string resultKey)
        {
            if (predicate(key))
            {
                resultKey = key;
                return true;
            }

            var childKeys = source.Query.GetChildNodeKeys(key);
            foreach (var childKey in childKeys)
            {
                if (TryFindResult(source, childKey, predicate, out resultKey))
                {
                    return true;
                }
            }

            resultKey = string.Empty;
            return false;
        }

        // public static bool IsRootCursorKey(this GraphExecutionContext context, string key)
        // {
        //     return context.Query.TryFind(k => HasCursor(context, k), context.Query.GetRootKey(), out var resultKey) && resultKey == key;
        // }

        // private static bool HasCursor(GraphExecutionContext context, string key)
        // {
        //     var node = context.Query.GetNode(key);
        //     return node.SupportsConnectionArguments() && !string.IsNullOrWhiteSpace(node.GetConnectionArguments().Cursor());
        // }
    }
}
