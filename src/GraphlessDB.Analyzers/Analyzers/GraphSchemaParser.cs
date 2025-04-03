/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using GraphlessDB.Collections.Immutable;

namespace GraphlessDB.Analyzers
{
    public sealed class GraphSchemaParser
    {
        public static readonly GraphSchemaParser Default = new();

#pragma warning disable CA1822 // Mark members as static
        public GraphSchema Parse(string namespaceValue, string name, string value)
#pragma warning restore CA1822 // Mark members as static
        {
            var enums = ImmutableList.CreateBuilder<EnumSchema>();
            var entities = ImmutableList.CreateBuilder<Entity>();
            using var reader = new StringReader(value);
            var state = SchemaState.Document;
            var enumSchema = (EnumSchema?)null;
            var node = (Node?)null;
            var edge = (Edge?)null;
            while (true)
            {
                var line = reader.ReadLine();
                if (line == null)
                {
                    break;
                }

                line = line.Trim();
                switch (state)
                {
                    case SchemaState.Document:
                        if (line.StartsWith("enum ", StringComparison.Ordinal) &&
                            line.EndsWith(" {", StringComparison.Ordinal))
                        {
                            state = SchemaState.Enum;
                            enumSchema = new EnumSchema(
                                line.Substring(5, line.Length - 7),
                                ImmutableListSequence<string>.Empty);
                            break;
                        }

                        if (line.StartsWith("node ", StringComparison.Ordinal) &&
                            line.EndsWith(" {", StringComparison.Ordinal))
                        {
                            state = SchemaState.Node;
                            node = new Node(
                                line.Substring(5, line.Length - 7),
                                ImmutableListSequence<EntityProperty>.Empty);
                            break;
                        }

                        if (line.StartsWith("edge ", StringComparison.Ordinal) &&
                            line.EndsWith(" {", StringComparison.Ordinal))
                        {
                            state = SchemaState.Edge;
                            edge = new Edge(
                                line.Substring(5, line.Length - 7),
                                new NodeRef(string.Empty, string.Empty),
                                new NodeRef(string.Empty, string.Empty),
                                ImmutableListSequence<EntityProperty>.Empty);
                            break;
                        }

                        throw new InvalidOperationException($"Invalid schema format in state {Enum.GetName(typeof(SchemaState), state)} '{line}'");
                    case SchemaState.Enum:
                        if (enumSchema == null)
                        {
                            throw new InvalidOperationException("Enum was missing");
                        }

                        if (line == "}")
                        {
                            state = SchemaState.Document;
                            enums.Add(enumSchema);
                            break;
                        }

                        var enumValueParts = line
                            .Split(':', ' ')
                            .Select(p => p.Trim())
                            .ToList();

                        if (enumValueParts.Count == 1)
                        {
                            enumSchema = enumSchema with
                            {
                                Values = enumSchema
                                    .Values
                                    .Items
                                    .Add(enumValueParts[0])
                                    .ToImmutableListSequence()
                            };
                            break;
                        }

                        throw new InvalidOperationException($"Invalid schema format in state {Enum.GetName(typeof(SchemaState), state)} '{line}'");
                    case SchemaState.Node:
                        if (node == null)
                        {
                            throw new InvalidOperationException("Node was missing");
                        }

                        if (line == "}")
                        {
                            state = SchemaState.Document;
                            entities.Add(node);
                            break;
                        }

                        var nodePropParts = line
                            .Split(':')
                            .Select(p => p.Trim())
                            .ToList();

                        if (nodePropParts.Count == 2)
                        {
                            node = node with
                            {
                                Properties = node
                                    .Properties
                                    .Items
                                    .Add(new EntityProperty(nodePropParts[0], nodePropParts[1]))
                                    .ToImmutableListSequence()
                            };
                            break;
                        }

                        throw new InvalidOperationException($"Invalid schema format in state {Enum.GetName(typeof(SchemaState), state)} '{line}'");
                    case SchemaState.Edge:
                        if (edge == null)
                        {
                            throw new InvalidOperationException("Node was missing");
                        }

                        if (line == "}")
                        {
                            state = SchemaState.Document;
                            entities.Add(edge);
                            break;
                        }

                        if (line.StartsWith("in ", StringComparison.Ordinal))
                        {
                            var edgeInNodeParts = line
                                .Substring(3)
                                .Split(':')
                                .Select(p => p.Trim())
                                .ToList();

                            if (edgeInNodeParts.Count != 2)
                            {
                                throw new InvalidOperationException($"Invalid schema format in state {Enum.GetName(typeof(SchemaState), state)} '{line}'");
                            }

                            edge = edge with
                            {
                                In = new NodeRef(edgeInNodeParts[0], edgeInNodeParts[1])
                            };
                            break;
                        }

                        if (line.StartsWith("out ", StringComparison.Ordinal))
                        {
                            var edgeOutNodeParts = line
                                .Substring(4)
                                .Split(':')
                                .Select(p => p.Trim())
                                .ToList();

                            if (edgeOutNodeParts.Count != 2)
                            {
                                throw new InvalidOperationException($"Invalid schema format in state {Enum.GetName(typeof(SchemaState), state)} '{line}'");
                            }

                            edge = edge with
                            {
                                Out = new NodeRef(edgeOutNodeParts[0], edgeOutNodeParts[1])
                            };
                            break;
                        }

                        var edgePropParts = line
                            .Split(':')
                            .Select(p => p.Trim())
                            .ToList();

                        if (edgePropParts.Count == 2)
                        {
                            edge = edge with
                            {
                                Properties = edge
                                    .Properties
                                    .Items
                                    .Add(new EntityProperty(edgePropParts[0], ParseType(edgePropParts[1])))
                                    .ToImmutableListSequence()
                            };
                            break;
                        }
                        throw new InvalidOperationException($"Invalid schema format in state {Enum.GetName(typeof(SchemaState), state)} '{line}'");
                    default:
                        throw new NotSupportedException();
                }
            }

            return new GraphSchema(
                namespaceValue,
                name,
                enums
                    .ToImmutableList()
                    .ToImmutableListSequence(),
                entities
                    .ToImmutableList()
                    .ToImmutableListSequence())
                    .ThrowIfInvalid();
        }

        private static string ParseType(string value)
        {
            return value;
            // return value switch
            // {
            //     "Id" => EntityPropertyType.Id,
            //     "String" => EntityPropertyType.String,
            //     "DateTime" => EntityPropertyType.DateTime,
            //     "Enum" => EntityPropertyType.Enum,
            //     _ => throw new NotSupportedException($"EntityPropertyType '{value}' not supported"),
            // };
        }
    }

    public enum SchemaState
    {
        Document,
        Enum,
        Node,
        Edge,
    }
}
