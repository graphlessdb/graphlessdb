/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Storage;
using Microsoft.Extensions.Logging;

namespace GraphlessDB.Domain.Graph.Services
{
    internal static partial class Log
    {
        [LoggerMessage(EventId = 0, Level = LogLevel.Warning, Message = "Could not covert RDFTriple to versioned node. RDFTriple={RDFTriple}")]
        internal static partial void CouldNotConvertRDFTripleToVersionedNode(this ILogger logger, RDFTriple rdfTriple);
    }
}
