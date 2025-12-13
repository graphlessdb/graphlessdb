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
    public static class ImmutableTreeExtensions
    {
        public static TKey? GetRootKeyOrDefault<TKey, TNode>(this ImmutableTree<TKey, TNode> source)
            where TKey : notnull
        {
            return source
                .Nodes
                .ByKey
                .Where(kv => !source.TryGetParentKey(kv.Key, out var _))
                .Select(kv => kv.Key)
                .SingleOrDefault();
        }

        public static TKey GetRootKey<TKey, TNode>(this ImmutableTree<TKey, TNode> source)
            where TKey : notnull
        {
            return source
                .Nodes
                .ByKey
                .Where(kv => !source.TryGetParentKey(kv.Key, out var _))
                .Select(kv => kv.Key)
                .Single();
        }

        public static TNode? GetRootNodeOrDefault<TKey, TNode>(this ImmutableTree<TKey, TNode> source)
            where TKey : notnull
        {
            var rootKey = source.GetRootKeyOrDefault();
            return rootKey == null
                ? default
                : source.Nodes.ByKey[rootKey];
        }

        public static TNode GetRootNode<TKey, TNode>(this ImmutableTree<TKey, TNode> source)
            where TKey : notnull
        {
            return source.Nodes.ByKey[source.GetRootKey()];
        }

        public static TNode GetNode<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key)
            where TKey : notnull
        {
            return source.Nodes.ByKey[key];
        }

        public static TNode? TryGetNode<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key)
            where TKey : notnull
        {
            if (source.Nodes.ByKey.TryGetValue(key, out var node))
            {
                return node;
            }

            return default;
        }

        public static TKey GetSingleChildNodeKey<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key)
            where TKey : notnull
        {
            return source.GetChildNodeKeys(key).Single();
        }

        public static TKey? GetSingleOrDefaultChildNodeKey<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key)
            where TKey : notnull
        {
            return source.GetChildNodeKeys(key).SingleOrDefault();
        }

        public static TNode GetSingleChildNode<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key)
            where TKey : notnull
        {
            return source.GetNode(source.GetSingleChildNodeKey(key));
        }

        public static TNode? TryGetParentNode<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key)
            where TKey : notnull
        {
            if (source.Edges.ByOutKey.TryGetValue(key, out var edges))
            {
                var parentKey = edges.Single().InKey;
                return source.GetNode(parentKey);
            }

            return default;
        }

        public static ImmutableList<TNode> GetChildNodes<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key)
            where TKey : notnull
        {
            return source
                .GetChildNodeKeys(key)
                .Select(k => source.GetNode(k))
                .ToImmutableList();
        }

        public static ImmutableList<TKey> GetChildNodeKeys<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key)
            where TKey : notnull
        {
            if (source.Edges.ByInKey.TryGetValue(key, out var edges))
            {
                return edges.Select(e => e.OutKey).ToImmutableList();
            }

            return [];
        }

        public static TKey GetParentKey<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key)
            where TKey : notnull
        {
            if (source.TryGetParentKey(key, out var parentKey))
            {
                return parentKey ?? throw new InvalidOperationException();
            }

            throw new InvalidOperationException();
        }

        public static bool TryGetParentKey<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key, out TKey? parentKey)
            where TKey : notnull
        {
            if (source.Edges.ByOutKey.TryGetValue(key, out var edges))
            {
                parentKey = edges.Select(e => e.InKey).Single();
                return true;
            }

            parentKey = default;
            return false;
        }

        public static ImmutableTree<TKey, TNode> SetNode<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key, TNode node)
            where TKey : notnull
        {
            return source with
            {
                Nodes = source.Nodes with
                {
                    ByKey = source.Nodes.ByKey.SetItem(key, node)
                }
            };
        }

        public static ImmutableTree<TKey, TNode> AddNode<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key, TNode node)
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

        public static ImmutableTree<TKey, TNode> AddParentNode<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey childKey, TKey key, TNode node)
            where TKey : notnull
        {
            return source
                .AddNode(key, node)
                .AddEdge(childKey, key);
        }

        public static ImmutableTree<TKey, TNode> AddChildNode<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey parentKey, TKey key, TNode node)
            where TKey : notnull
        {
            return source
                .AddNode(key, node)
                .AddEdge(key, parentKey);
        }

        public static ImmutableTree<TKey, TNode> GetSubTree<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key)
            where TKey : notnull
        {
            var node = source.GetNode(key);
            var target = ImmutableTree<TKey, TNode>
                .Empty
                .AddNode(key, node);

            var childKeys = source.GetChildNodeKeys(key);
            foreach (var childKey in childKeys)
            {
                target = GetSubTree(source, key, childKey, target);
            }

            return target;
        }

        private static ImmutableTree<TKey, TNode> GetSubTree<TKey, TNode>(ImmutableTree<TKey, TNode> source, TKey parentKey, TKey key, ImmutableTree<TKey, TNode> target)
            where TKey : notnull
        {
            var node = source.GetNode(key);
            target = target.AddChildNode(parentKey, key, node);
            var childKeys = source.GetChildNodeKeys(key);
            foreach (var childKey in childKeys)
            {
                target = GetSubTree(source, key, childKey, target);
            }

            return target;
        }

        public static ImmutableTree<TKey, TNode> AddSubTree<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey key, ImmutableTree<TKey, TNode> childTree)
            where TKey : notnull
        {
            // Copy across the nodes
            foreach (var kv in childTree.Nodes.ByKey)
            {
                source = source.AddNode(kv.Key, kv.Value);
            }

            // Copy across the edges
            foreach (var edge in childTree.Edges.ByInKey.Values.SelectMany(v => v))
            {
                source = source.AddEdge(edge.OutKey, edge.InKey);
            }

            // Connect the two trees
            var childTreeRootKey = childTree.GetRootKey();
            source = source.AddEdge(childTreeRootKey, key);

            return source;
        }

        public static ImmutableTree<TKey, TNode> AddEdge<TKey, TNode>(this ImmutableTree<TKey, TNode> source, TKey childOutKey, TKey parentInKey)
            where TKey : notnull
        {
            if (!source.Nodes.ByKey.ContainsKey(parentInKey) || !source.Nodes.ByKey.ContainsKey(childOutKey))
            {
                throw new ArgumentException("Referenced node was not found");
            }

            var edge = new KeyPair<TKey>(parentInKey, childOutKey);

            return source with
            {
                Edges = source.Edges with
                {
                    ByInKey = GetByInKey(source, edge),
                    ByOutKey = GetByOutKey(source, edge)
                }
            };
        }

        private static ImmutableDictionary<TKey, ImmutableList<KeyPair<TKey>>> GetByOutKey<TKey, TNode>(ImmutableTree<TKey, TNode> source, KeyPair<TKey> edge) where TKey : notnull
        {
            if (source.Edges.ByOutKey.TryGetValue(edge.OutKey, out var value))
            {
                return source.Edges.ByOutKey.SetItem(edge.OutKey, value.Add(edge));
            }

            return source.Edges.ByOutKey.Add(edge.OutKey, [edge]);
        }

        private static ImmutableDictionary<TKey, ImmutableList<KeyPair<TKey>>> GetByInKey<TKey, TNode>(ImmutableTree<TKey, TNode> source, KeyPair<TKey> edge) where TKey : notnull
        {
            if (source.Edges.ByInKey.TryGetValue(edge.InKey, out var value))
            {
                return source.Edges.ByInKey.SetItem(edge.InKey, value.Add(edge));
            }

            return source.Edges.ByInKey.Add(edge.InKey, [edge]);
        }

        public static bool TryFind<TKey, TNode>(this ImmutableTree<TKey, TNode> source, Func<TKey, bool> predicate, TKey key, out TKey resultKey)
            where TKey : notnull
        {
            if (predicate(key))
            {
                resultKey = key;
                return true;
            }

            var childKeys = source.GetChildNodeKeys(key);
            foreach (var childKey in childKeys)
            {
                if (source.TryFind(predicate, childKey, out resultKey))
                {
                    return true;
                }
            }

            resultKey = key;
            return false;
        }
    }
}
