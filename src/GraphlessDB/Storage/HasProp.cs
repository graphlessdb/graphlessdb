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
    public sealed class HasProp : IPredicate
    {
        public const string Name = "prop";

        // NOTE Subject is required for sorting when query results have the same propertyValue
        public HasProp(string graphName, string typeName, string propertyName, string propertyValue, string subject)
        {
            if (string.IsNullOrWhiteSpace(graphName) || graphName.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(graphName));
            }

            if (string.IsNullOrWhiteSpace(typeName) || typeName.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(typeName));
            }

            if (string.IsNullOrWhiteSpace(propertyName) || propertyName.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(propertyName));
            }

            if (propertyValue == null || propertyValue.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(propertyValue));
            }

            if (string.IsNullOrWhiteSpace(subject) || subject.Contains('#'))
            {
                throw new ArgumentException("Cannot be null, empty, whitespace or contain #", nameof(subject));
            }

            GraphName = graphName;
            TypeName = typeName;
            PropertyName = propertyName;
            PropertyValue = propertyValue;
            Subject = subject;
        }

        public string GraphName { get; }

        public string TypeName { get; }

        public string PropertyName { get; }

        public string PropertyValue { get; }

        public string Subject { get; }

        public static bool IsPredicate(string value)
        {
            var parts = value.Split('#');
            return parts.Length >= 6 && parts[1] == Name;
        }

        public static HasProp Parse(string value)
        {
            var parts = value.Split('#');
            if (parts.Length != 6 || parts[1] != Name)
            {
                throw new GraphlessDBOperationException("Invalid format");
            }

            return new HasProp(parts[0], parts[2], parts[3], parts[4], parts[5]);
        }

        public override string ToString()
        {
            return $"{GraphName}#{Name}#{TypeName}#{PropertyName}#{PropertyValue}#{Subject}";
        }

        public static string PropertiesByType(string graphName, string typeName)
        {
            return $"{graphName}#{Name}#{typeName}#";
        }

        public static string PropertiesByTypeAndPropertyName(string graphName, string typeName, string propertyName)
        {
            return $"{graphName}#{Name}#{typeName}#{propertyName}#";
        }

        // TODO This implicitly does a BeginsWith search.  It should be split into two and have an Equals version also
        public static string PropertiesByTypePropertyNameAndValue(string graphName, string typeName, string propertyName, string propertyValue)
        {
            return $"{graphName}#{Name}#{typeName}#{propertyName}#{propertyValue}";
        }
    }
}