/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.Storage
{
    public sealed class HasOutEdge : IPredicate
    {
        public const string Name = "out";

        public HasOutEdge(string graphName, string nodeOutTypeName, string edgeTypeName, string nodeInId, string nodeOutId)
        {
            if (string.IsNullOrWhiteSpace(graphName) || graphName.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(graphName));
            }

            if (string.IsNullOrWhiteSpace(nodeOutTypeName) || nodeOutTypeName.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(nodeOutTypeName));
            }

            if (string.IsNullOrWhiteSpace(edgeTypeName) || edgeTypeName.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(edgeTypeName));
            }

            if (string.IsNullOrWhiteSpace(nodeInId) || nodeInId.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(nodeInId));
            }

            if (string.IsNullOrWhiteSpace(nodeOutId) || nodeOutId.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(nodeOutId));
            }

            GraphName = graphName;
            NodeOutTypeName = nodeOutTypeName;
            EdgeTypeName = edgeTypeName;
            NodeInId = nodeInId;
            NodeOutId = nodeOutId;
        }

        public string GraphName { get; }

        public string NodeOutTypeName { get; }

        public string EdgeTypeName { get; }

        public string NodeInId { get; }

        public string NodeOutId { get; }

        public override string ToString()
        {
            return $"{GraphName}#{Name}#{NodeOutTypeName}#{EdgeTypeName}#{NodeInId}#{NodeOutId}";
        }

        public static bool IsPredicate(string value)
        {
            var parts = value.Split('#');
            return parts.Length == 6 && parts[1] == Name;
        }

        public static HasOutEdge Parse(string value)
        {
            var parts = value.Split('#');
            if (parts.Length != 6 || parts[1] != Name)
            {
                throw new ArgumentException("Failed to parse predicate");
            }

            return new HasOutEdge(parts[0], parts[2], parts[3], parts[4], parts[5]);
        }

        public static string EdgesByTypeNodeOutType(string graphName, string nodeOutTypeName)
        {
            return $"{graphName}#{Name}#{nodeOutTypeName}#";
        }

        public static string EdgesByTypeNodeOutTypeAndEdgeType(string graphName, string nodeOutTypeName, string edgeTypeName)
        {
            return $"{graphName}#{Name}#{nodeOutTypeName}#{edgeTypeName}#";
        }
    }
}