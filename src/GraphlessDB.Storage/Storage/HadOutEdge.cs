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
    public sealed class HadOutEdge : IPredicate
    {
        public const string Name = "hadOut";

        public HadOutEdge(string graphName, string nodeOutTypeName, string edgeTypeName, string nodeInId, string nodeOutId, DateTime createdAt, DateTime deletedAt)
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
            CreatedAt = createdAt;
            DeletedAt = deletedAt;
        }

        public string GraphName { get; }

        public string NodeOutTypeName { get; }

        public string EdgeTypeName { get; }

        public string NodeInId { get; }

        public string NodeOutId { get; }

        public DateTime CreatedAt { get; }

        public DateTime DeletedAt { get; }

        public override string ToString()
        {
            return $"{GraphName}#{Name}#{NodeOutTypeName}#{EdgeTypeName}#{NodeInId}#{NodeOutId}#{CreatedAt:u}#{DeletedAt:u}";
        }
    }
}