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
using GraphlessDB.Collections;
using GraphlessDB.Query;

namespace GraphlessDB
{
    public static class GraphQueryExtensions
    {
        public static ImmutableTree<string, GraphQueryNode> WithConsistentRead(
            this ImmutableTree<string, GraphQueryNode> source, bool value)
        {
            return source with
            {
                Nodes = source.Nodes with
                {
                    ByKey = source
                    .Nodes
                    .ByKey
                    .Select(kv => new KeyValuePair<string, GraphQueryNode>(kv.Key, kv.Value.WithConsistentRead(value)))
                    .ToImmutableDictionary()
                }
            };
        }

        public static bool TryGetRootConnectionArgumentsKey(this ImmutableTree<string, GraphQueryNode> source, out string resultKey)
        {
            return source.TryFind(k => source.GetNode(k).SupportsConnectionArguments(), source.GetRootKey(), out resultKey);
        }

        public static bool IsRootConnectionArgumentsKey(this ImmutableTree<string, GraphQueryNode> source, string key)
        {
            return source.TryGetRootConnectionArgumentsKey(out var resultKey) && resultKey == key;
        }

        public static ImmutableTree<string, GraphQueryNode> WithIntermediateConnectionSize(
            this ImmutableTree<string, GraphQueryNode> source, int value)
        {
            foreach (var key in source.Nodes.ByKey.Keys)
            {
                if (source.GetNode(key).SupportsConnectionArguments() && !source.IsRootConnectionArgumentsKey(key))
                {
                    var node = source.Nodes.ByKey[key].WithConnectionSize(value);
                    source = source with
                    {
                        Nodes = source.Nodes with
                        {
                            ByKey = source.Nodes.ByKey.SetItem(key, node)
                        }
                    };
                }
            }

            return source;
        }

        public static ImmutableTree<string, GraphQueryNode> WithPreFilteredConnectionSize(
            this ImmutableTree<string, GraphQueryNode> source, int value)
        {
            foreach (var key in source.Nodes.ByKey.Keys)
            {
                if (source.GetNode(key).SupportsConnectionArguments())
                {
                    var node = source.Nodes.ByKey[key].WithPreFilteredConnectionSize(value);
                    source = source with
                    {
                        Nodes = source.Nodes with
                        {
                            ByKey = source.Nodes.ByKey.SetItem(key, node)
                        }
                    };
                }
            }

            return source;
        }

        public static ImmutableTree<string, GraphQueryNode> WithConnectionArguments(
            this ImmutableTree<string, GraphQueryNode> source,
            ConnectionArguments value)
        {
            if (!source.TryGetRootConnectionArgumentsKey(out var rootKey))
            {
                return source;
            }

            return source with
            {
                Nodes = source.Nodes with
                {
                    ByKey = source.Nodes.ByKey.SetItem(
                        rootKey,
                        source.GetNode(rootKey).WithConnectionArguments(value))
                }
            };
        }

        public static bool HasCursor(this ImmutableTree<string, GraphQueryNode> source)
        {
            return source
                .Nodes
                .ByKey
                .Values
                .Where(v => v.SupportsConnectionArguments() && v.GetConnectionArguments().After != null)
                .Any();
        }
    }
}
