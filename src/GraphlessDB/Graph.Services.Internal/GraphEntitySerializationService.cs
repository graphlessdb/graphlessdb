/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using GraphlessDB.Storage;
using Microsoft.Extensions.Options;

namespace GraphlessDB.Graph.Services.Internal
{
    internal sealed class GraphEntitySerializationService(IOptions<GraphEntitySerializationServiceOptions> options) : IGraphEntitySerializationService
    {
        public IEdge DeserializeEdge(string value, Type type)
        {
            return (IEdge?)JsonSerializer.Deserialize(value, type, GetJsonContext(type)) ?? throw new GraphlessDBOperationException("Failed to deserialize edge");
        }

        public INode DeserializeNode(string value, Type type)
        {
            return (INode?)JsonSerializer.Deserialize(value, type, GetJsonContext(type)) ?? throw new GraphlessDBOperationException("Failed to deserialize node");
        }

        public string SerializeNode(INode node, Type type)
        {
            return JsonSerializer.Serialize(node, type, GetJsonContext(type));
        }

        public string SerializeEdge(IEdge edge, Type type)
        {
            return JsonSerializer.Serialize(edge, type, GetJsonContext(type));
        }

        private JsonSerializerContext GetJsonContext(Type type)
        {
            var o = options.Value;
            if (o.JsonContextOverrides.TryGetValue(type, out var overrideJsonContext))
            {
                return overrideJsonContext;
            }

            return o.JsonContext ?? throw new GraphlessDBOperationException("EntitySerializerOptions.JsonContext was not set");
        }
    }
}
