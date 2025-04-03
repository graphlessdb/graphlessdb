/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Text;
using System.Text.Json;

namespace GraphlessDB.Graph.Services.Internal
{
    internal sealed class GraphCursorSerializationService : IGraphCursorSerializationService
    {
        public string Serialize(Cursor value)
        {
            var json = JsonSerializer.Serialize(value, GraphCursorJsonSerializerContext.Default.Cursor);
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(json));
        }

        public Cursor Deserialize(string value)
        {
            var json = Encoding.UTF8.GetString(Convert.FromBase64String(value));
            return JsonSerializer.Deserialize(json, GraphCursorJsonSerializerContext.Default.Cursor) ?? throw new GraphlessDBOperationException("Failed to deserialize cursor");
        }
    }
}
