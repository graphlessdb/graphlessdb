/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.Graph.Services.Internal
{
    internal sealed class GraphSerializationService : IGraphSerializationService
    {
        public string GetPropertyAsString(object? value)
        {
            return value switch
            {
                DateTime dateValue => SortableInt64String.ToString(dateValue.Ticks),
                short shortValue => SortableInt16String.ToString(shortValue),
                int intValue => SortableInt32String.ToString(intValue),
                long longValue => SortableInt64String.ToString(longValue),
                float floatValue => SortableSingleString.ToString(floatValue),
                double doubleValue => SortableDoubleString.ToString(doubleValue),
                decimal decimalValue => SortableDecimalString.ToString(decimalValue),
                _ => value?.ToString() ?? string.Empty
            };
        }
    }
}
