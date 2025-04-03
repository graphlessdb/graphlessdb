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
    public sealed class HasInEdgeProp : IPredicate
    {
        public const string Name = "inProp";

        public HasInEdgeProp(string graphName, string nodeInTypeName, string edgeTypeName, string propertyName, string propertyValue, string nodeInId, string nodeOutId)
        {
            if (string.IsNullOrWhiteSpace(graphName) || graphName.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(graphName));
            }

            if (string.IsNullOrWhiteSpace(nodeInTypeName) || nodeInTypeName.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(nodeInTypeName));
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
            NodeInTypeName = nodeInTypeName;
            EdgeTypeName = edgeTypeName;
            PropertyName = propertyName;
            PropertyValue = propertyValue;
            NodeInId = nodeInId;
            NodeOutId = nodeOutId;
        }

        public string GraphName { get; }

        public string NodeInTypeName { get; }

        public string EdgeTypeName { get; }

        public string PropertyName { get; }

        public string PropertyValue { get; }

        public string NodeInId { get; }

        public string NodeOutId { get; }

        public override string ToString()
        {
            return $"{GraphName}#{Name}#{NodeInTypeName}#{EdgeTypeName}#{PropertyName}#{PropertyValue}#{NodeInId}#{NodeOutId}";
        }

        public static bool IsPredicate(string value)
        {
            var parts = value.Split('#');
            return parts.Length >= 8 && parts[1] == Name;
        }

        public static HasInEdgeProp Parse(string value)
        {
            var parts = value.Split('#');
            if (parts.Length < 8 || parts[1] != Name)
            {
                throw new ArgumentException("Failed to parse predicate");
            }

            return new HasInEdgeProp(parts[0], parts[2], parts[3], parts[4], string.Join('#', parts[5..^2]), parts[^2], parts[^1]);
        }

        public static string EdgesByTypeNodeInTypeAndEdgeType(string graphName, string nodeInTypeName, string edgeTypeName)
        {
            return $"{graphName}#{Name}#{nodeInTypeName}#{edgeTypeName}#";
        }

        public static string EdgesByTypeNodeInTypeEdgeTypeAndPropertyName(string graphName, string nodeInTypeName, string edgeTypeName, string propertyName)
        {
            return $"{graphName}#{Name}#{nodeInTypeName}#{edgeTypeName}#{propertyName}#";
        }

        public static string EdgesByTypeNodeInTypeEdgeTypeAndPropertyNameAndBeginsWithValue(string graphName, string nodeInTypeName, string edgeTypeName, string propertyName, PropertyOperator propertyOperator, string propertyValue)
        {
            return propertyOperator == PropertyOperator.Equals
                ? $"{graphName}#{Name}#{nodeInTypeName}#{edgeTypeName}#{propertyName}#{propertyValue}#"
                : $"{graphName}#{Name}#{nodeInTypeName}#{edgeTypeName}#{propertyName}#{propertyValue}";
        }
    }
}