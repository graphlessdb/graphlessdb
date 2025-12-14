using GraphlessDB.Domain.Graph.Services;


/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */
namespace GraphlessDB.Query.Services
{
    internal sealed class EmptyGraphQueryablePropertyService : IGraphQueryablePropertyService
    {
        public bool IsQueryableProperty(string typeName, string propertyName)
        {
            return false;
        }
    }
}
