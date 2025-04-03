/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Globalization;

namespace GraphlessDB.Storage
{
    public sealed class HasBlob : IPredicate
    {
        public const string Name = "blob";

        public HasBlob(string graphName, string typeName, int version)
        {
            if (string.IsNullOrWhiteSpace(graphName) || graphName.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(graphName));
            }

            if (string.IsNullOrWhiteSpace(typeName) || typeName.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(typeName));
            }

            GraphName = graphName;
            TypeName = typeName;
            Version = version;
        }

        public string GraphName { get; }

        public string TypeName { get; }

        public int Version { get; }

        public override string ToString()
        {
            return $"{GraphName}#{Name}#{TypeName}#{Version}";
        }

        public static bool IsHasBlob(string graphName, string value)
        {
            return value.StartsWith($"{graphName}#${Name}#", StringComparison.Ordinal);
        }

        public static string IsHasBlobWithType(string graphName, string typeName)
        {
            return $"{graphName}#{Name}#{typeName}#";
        }

        public static HasBlob Parse(string value)
        {
            var parts = value.Split('#');
            if (parts.Length != 4 || parts[1] != Name)
            {
                throw new ArgumentException("Failed to parse predicate");
            }

            return new HasBlob(parts[0], parts[2], int.Parse(parts[3], NumberStyles.Integer, CultureInfo.InvariantCulture));
        }
    }
}