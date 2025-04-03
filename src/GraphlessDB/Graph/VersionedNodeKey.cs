/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.Graph
{
    public sealed class VersionedNodeKey
    {
        public VersionedNodeKey(string id, int version)
        {
            // if (string.IsNullOrWhiteSpace(typeName))
            // {
            //     throw new ArgumentException("Cannot be null, empty or whitespace", nameof(typeName));
            // }

            if (string.IsNullOrWhiteSpace(id))
            {
                throw new ArgumentException("Cannot be null, empty or whitespace", nameof(id));
            }

            if (version < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(version), "Cannot be less than zero");
            }

            // TypeName = typeName;
            Id = id;
            Version = version;
        }

        // public string TypeName { get; }

        public string Id { get; }

        public int Version { get; }
    }
}