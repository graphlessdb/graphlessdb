/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.Graph.Services
{
    public sealed class TestGraphDBTypeMapper : IGraphEntityTypeService
    {
        public Type GetEntityType(string typeName)
        {
#pragma warning disable IL2057 // Unrecognized value passed to the parameter of method. It's not possible to guarantee the availability of the target type.
            var type = Type.GetType($"GraphlessDB.Tests.{typeName}, GraphlessDB.Tests");
#pragma warning restore IL2057 // Unrecognized value passed to the parameter of method. It's not possible to guarantee the availability of the target type.
            return type ?? throw new InvalidOperationException($"A type for {typeName} was not found");
        }
    }
}
