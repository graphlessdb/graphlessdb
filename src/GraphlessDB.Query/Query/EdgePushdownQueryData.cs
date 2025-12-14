/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Domain.Graph;
using GraphlessDB.Domain;
using GraphlessDB.Domain.Services;
using GraphlessDB;

namespace GraphlessDB.Query
{
    public sealed record EdgePushdownQueryData(
        OrderArguments? Order,
        EdgeFilterArguments? Filter);
}
