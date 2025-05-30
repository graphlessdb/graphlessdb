/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Query.Services;

namespace GraphlessDB
{
    public sealed record GraphDB(
        IGraphQueryExecutionService QueryExecutionService,
        IGraphNodeFilterService NodeFilterService,
        IGraphHouseKeepingService HouseKeepingService) : IGraphDB;
}
