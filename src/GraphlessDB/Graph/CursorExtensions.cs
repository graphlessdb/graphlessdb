/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using System.Linq;
using GraphlessDB;
using GraphlessDB.Collections;
using GraphlessDB.Graph;

namespace GraphlessDB.Graph
{
    public static class CursorExtensions
    {
        public static int GetNodeCount(this Cursor source)
        {
            return source.Items.Nodes.ByKey.Count;
        }

        public static string GetRootKey(this Cursor source)
        {
            return source.Items.GetRootKey();
        }

        public static CursorNode GetRootNode(this Cursor source)
        {
            return source.Items.GetRootNode();
        }

        public static CursorNode? GetRootNodeOrDefault(this Cursor source)
        {
            return source.Items.GetRootNodeOrDefault();
        }

        public static ImmutableList<string> GetChildNodeKeys(this Cursor source, string key)
        {
            return source.Items.GetChildNodeKeys(key);
        }

        public static ImmutableList<CursorNode> GetChildNodes(this Cursor source, string key)
        {
            return source.Items.GetChildNodes(key);
        }

        public static ImmutableList<Cursor> GetSubTrees(this Cursor source, string key)
        {
            return source
                .Items
                .GetChildNodeKeys(key)
                .Select(source.GetSubTree)
                .ToImmutableList();
        }

        public static Cursor GetSubTree(this Cursor source, string nodeKey)
        {
            return GetSubTree(source, null, nodeKey, source.Items.GetNode(nodeKey), (string?)null);
        }

        private static Cursor GetSubTree(Cursor source, Cursor? target, string childNodeKey, CursorNode childNode, string? parentNodeKey)
        {
            var targetChildNodeKey = (string?)null;
            if (target != null)
            {
                target = target.AddChildNode(childNode, parentNodeKey ?? throw new GraphlessDBOperationException("ParentNodeKey was missing"), out targetChildNodeKey);
            }
            else
            {
                target = Cursor.Create(childNode);
                targetChildNodeKey = target.GetRootKey();
            }

            target = source
                .GetChildNodeKeys(childNodeKey)
                .Aggregate(target, (agg, cur) => GetSubTree(source, target, cur, source.Items.GetNode(cur), targetChildNodeKey));

            return target;
        }

        public static Cursor AddAsParentToRoot(this Cursor source, CursorNode value)
        {
            var newCursor = Cursor.Create(value);
            return newCursor
                .AddSubTree(source, newCursor.GetRootKey());
        }

        public static Cursor AddSubTree(this Cursor source, Cursor childCursor, string parentNodeKey)
        {
            var childCursorRootNodeKey = childCursor.GetRootKey();
            var childCursorRootNode = childCursor.GetRootNode();
            var cursor = source.AddChildNode(childCursorRootNode, parentNodeKey, out var targetChildNodeKey);
            var childCursorRootNodeChildNodes = childCursor.GetSubTrees(childCursorRootNodeKey);
            foreach (var childCursorRootNodeChildNode in childCursorRootNodeChildNodes)
            {
                cursor = cursor.AddSubTree(childCursorRootNodeChildNode, targetChildNodeKey);
            }

            return cursor;
        }

        public static Cursor AddChildNode(this Cursor source, CursorNode childNode, string parentNodeKey, out string childNodeKey)
        {
            var childCount = source
                .Items
                .GetChildNodeKeys(parentNodeKey)
                .Count;

            childNodeKey = $"{parentNodeKey[..^1]},{childCount}]";
            return new Cursor(source.Items.AddChildNode(parentNodeKey, childNodeKey, childNode));
        }
    }
}
