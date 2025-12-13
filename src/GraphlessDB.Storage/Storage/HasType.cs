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
    public sealed class HasType : IPredicate
    {
        public const string Name = "type";

        public HasType(string graphName, string typeName, string subject)
        {
            if (string.IsNullOrWhiteSpace(graphName) || graphName.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(graphName));
            }

            if (string.IsNullOrWhiteSpace(typeName) || typeName.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(typeName));
            }

            if (string.IsNullOrWhiteSpace(subject) || subject.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(subject));
            }

            // NOTE Subject is required for explicit ordering of type records
            GraphName = graphName;
            TypeName = typeName;
            Subject = subject;
        }

        public string GraphName { get; }

        public string TypeName { get; }

        public string Subject { get; }

        public static HasType Parse(string value)
        {
            var parts = value.Split('#');
            if (parts.Length != 4 || parts[1] != Name)
            {
                throw new GraphlessDBOperationException("Invalid format");
            }

            return new HasType(parts[0], parts[2], parts[3]);
        }

        public override string ToString()
        {
            return $"{GraphName}#{Name}#{TypeName}#{Subject}";
        }

        public static bool IsPredicate(string value)
        {
            var parts = value.Split('#');
            return parts.Length == 4 && parts[1] == Name;
        }

        public static string ByGraphName(string graphName)
        {
            return $"{graphName}#{Name}#";
        }

        public static string ByType(string graphName, string typeName)
        {
            return $"{graphName}#{Name}#{typeName}#";
        }
    }
}