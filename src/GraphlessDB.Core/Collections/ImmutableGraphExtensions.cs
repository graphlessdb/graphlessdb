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

namespace GraphlessDB.Collections
{
    public static class ImmutableGraphExtensions
    {
        public static ImmutableList<TKey> GetRootKeys<TNode, TEdge, TKey>(this ImmutableGraph<TNode, TEdge, TKey> source)
            where TKey : notnull
        {
            return source
                .Nodes
                .ByKey
                .Where(kv => !source.Edges.ByInKey.ContainsKey(kv.Key))
                .Select(kv => kv.Key)
                .ToImmutableList();
        }

        public static ImmutableList<TNode> GetRootNodes<TNode, TEdge, TKey>(this ImmutableGraph<TNode, TEdge, TKey> source)
            where TKey : notnull
        {
            return source
                .GetRootKeys()
                .Select(k => source.Nodes.ByKey[k])
                .ToImmutableList();
        }

        public static ImmutableGraph<TNode, TEdge, TKey> AddNode<TNode, TEdge, TKey>(this ImmutableGraph<TNode, TEdge, TKey> source, TKey key, TNode node)
            where TKey : notnull
        {
            return source with
            {
                Nodes = source.Nodes with
                {
                    ByKey = source.Nodes.ByKey.Add(key, node)
                }
            };
        }

        public static ImmutableGraph<TNode, TEdge, TKey> AddEdge<TNode, TEdge, TKey>(this ImmutableGraph<TNode, TEdge, TKey> source, TKey inKey, TKey outKey, TEdge edge)
            where TKey : notnull
        {
            if (!source.Nodes.ByKey.ContainsKey(inKey) || !source.Nodes.ByKey.ContainsKey(outKey))
            {
                throw new ArgumentException("Referenced node was not found");
            }

            return source with
            {
                Edges = source.Edges with
                {
                    ByInKey = GetByInKey(source, inKey, edge),
                    ByOutKey = GetByOutKey(source, outKey, edge)
                }
            };
        }

        private static ImmutableDictionary<TKey, ImmutableList<TEdge>> GetByOutKey<TNode, TEdge, TKey>(ImmutableGraph<TNode, TEdge, TKey> source, TKey outKey, TEdge edge) where TKey : notnull
        {
            if (source.Edges.ByOutKey.TryGetValue(outKey, out var value))
            {
                return source.Edges.ByOutKey.SetItem(outKey, value.Add(edge));
            }

            return source.Edges.ByOutKey.Add(outKey, [edge]);
        }

        private static ImmutableDictionary<TKey, ImmutableList<TEdge>> GetByInKey<TNode, TEdge, TKey>(ImmutableGraph<TNode, TEdge, TKey> source, TKey inKey, TEdge edge) where TKey : notnull
        {
            if (source.Edges.ByInKey.TryGetValue(inKey, out var value))
            {
                return source.Edges.ByInKey.SetItem(inKey, value.Add(edge));
            }

            return source.Edges.ByInKey.Add(inKey, [edge]);
        }
    }
}
