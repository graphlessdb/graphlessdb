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
    public sealed class HasOutEdgeProp : IPredicate
    {
        public const string Name = "outProp";

        public HasOutEdgeProp(string graphName, string nodeOutTypeName, string edgeTypeName, string propertyName, string propertyValue, string nodeInId, string nodeOutId)
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

            if (string.IsNullOrWhiteSpace(propertyName) || propertyName.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(propertyName));
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
            PropertyName = propertyName;
            PropertyValue = propertyValue;
            NodeInId = nodeInId;
            NodeOutId = nodeOutId;
        }

        public string GraphName { get; }

        public string NodeOutTypeName { get; }

        public string EdgeTypeName { get; }

        public string PropertyName { get; }

        public string PropertyValue { get; }

        public string NodeInId { get; }

        public string NodeOutId { get; }

        public override string ToString()
        {
            return $"{GraphName}#{Name}#{NodeOutTypeName}#{EdgeTypeName}#{PropertyName}#{PropertyValue}#{NodeInId}#{NodeOutId}";
        }

        public static bool IsPredicate(string value)
        {
            var parts = value.Split('#');
            return parts.Length >= 8 && parts[1] == Name;
        }

        public static HasOutEdgeProp Parse(string value)
        {
            var parts = value.Split('#');
            if (parts.Length < 8 || parts[1] != Name)
            {
                throw new ArgumentException("Failed to parse predicate");
            }

            return new HasOutEdgeProp(parts[0], parts[2], parts[3], parts[4], string.Join('#', parts[5..^2]), parts[^2], parts[^1]);
        }

        public static string EdgesByTypeNodeOutTypeAndEdgeType(string graphName, string nodeOutTypeName, string edgeTypeName)
        {
            return $"{graphName}#{Name}#{nodeOutTypeName}#{edgeTypeName}#";
        }

        public static string EdgesByTypeNodeOutTypeEdgeTypeAndPropertyName(string graphName, string nodeOutTypeName, string edgeTypeName, string propertyName)
        {
            return $"{graphName}#{Name}#{nodeOutTypeName}#{edgeTypeName}#{propertyName}#";
        }

        public static string EdgesByTypeNodeOutTypeEdgeTypeAndPropertyNameAndBeginsWithValue(string graphName, string nodeOutTypeName, string edgeTypeName, string propertyName, PropertyOperator propertyOperator, string propertyValue)
        {
            return propertyOperator == PropertyOperator.Equals
                ? $"{graphName}#{Name}#{nodeOutTypeName}#{edgeTypeName}#{propertyName}#{propertyValue}#"
                : $"{graphName}#{Name}#{nodeOutTypeName}#{edgeTypeName}#{propertyName}#{propertyValue}";
        }
    }
}