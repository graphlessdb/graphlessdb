/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Linq;
using GraphlessDB.Collections.Immutable;

namespace GraphlessDB.Analyzers
{
    public static class GraphSchemaExtensions
    {
        public static GraphSchema Add(this GraphSchema source, GraphSchema other)
        {
            return new GraphSchema(
                other.Namespace,
                other.Name,
                source
                    .Enums
                    .Items
                    .AddRange(other.Enums.Items)
                    .ToImmutableListSequence(),
                source
                    .Entities
                    .Items
                    .AddRange(other.Entities.Items)
                    .ToImmutableListSequence());
        }

        public static GraphSchema ThrowIfInvalid(this GraphSchema source)
        {
            if (string.IsNullOrWhiteSpace(source.Namespace))
            {
                throw new InvalidOperationException("Graph Namespace was null or whitespace");
            }

            if (string.IsNullOrWhiteSpace(source.Name))
            {
                throw new InvalidOperationException("Graph Name was null or whitespace");
            }

            var duplicateEdgeFriendlyName = source
                .Entities
                .Items
                .OfType<Edge>()
                .SelectMany(e => new[] {
                     new Tuple<string, string, string>(e.In.NodeName, e.Out.FriendlyName, e.Name),
                     new Tuple<string, string, string>(e.Out.NodeName, e.In.FriendlyName, e.Name) })
                .GroupBy(v => $"{v.Item1}.{v.Item2}")
                .Where(v => v.Count() > 1)
                .Select(v => new Tuple<string, List<string>>(v.Key, v.Select(vv => vv.Item3).ToList()))
                .FirstOrDefault();

            if (duplicateEdgeFriendlyName != null)
            {
                throw new InvalidOperationException($"Graph node had duplicate edge name '{duplicateEdgeFriendlyName.Item1} on '{string.Join(",", duplicateEdgeFriendlyName.Item2)}''");
            }

            return source;
        }
    }
}
