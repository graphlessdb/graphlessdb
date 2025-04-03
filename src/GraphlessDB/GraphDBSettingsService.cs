/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Graph.Services;
using Microsoft.Extensions.Options;

namespace GraphlessDB
{
    public sealed class GraphDBSettingsService(IOptions<GraphOptions> options) : IGraphSettingsService
    {
        public GraphSettings GetGraphSettings()
        {
            return new GraphSettings(
                options.Value.TableName,
                options.Value.GraphName,
                options.Value.PartitionCount,
                options.Value.ByPredicateIndexName,
                options.Value.ByObjectIndexName);
        }
    }
}
