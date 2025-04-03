/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Storage;
using Microsoft.Extensions.Logging;

namespace GraphlessDB.Logging
{
    internal static partial class Log
    {
        [LoggerMessage(EventId = 0, Level = LogLevel.Warning, Message = "Could not covert RDFTriple to versioned node. RDFTriple={RDFTriple}")]
        internal static partial void CouldNotConvertRDFTripleToVersionedNode(this ILogger logger, RDFTriple rdfTriple);

        [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "ProvisionedThroughputExceededException caught, will retry. RetryIntervalSeconds={RetryIntervalSeconds}")]
        internal static partial void ProvisionedThroughputExceededExceptionCaughtWillRetry(this ILogger logger, double retryIntervalSeconds);

        [LoggerMessage(EventId = 3, Level = LogLevel.Warning, Message = "GraphConcurrencyException caught, will retry. RetryIntervalSeconds={RetryIntervalSeconds}")]
        internal static partial void GraphConcurrencyExceptionCaughtWillRetry(this ILogger logger, double retryIntervalSeconds);

        [LoggerMessage(EventId = 4, Level = LogLevel.Warning, Message = "Dangling edge detected. Id={Id}")]
        internal static partial void DanglingEdgeDetected(this ILogger logger, string id);

        [LoggerMessage(EventId = 5, Level = LogLevel.Warning, Message = "HttpRequestException caught, will retry. RetryIntervalSeconds={RetryIntervalSeconds}")]
        internal static partial void HttpRequestExceptionCaughtWillRetry(this ILogger logger, double retryIntervalSeconds);

        [LoggerMessage(EventId = 6, Level = LogLevel.Warning, Message = "Graph query service failure will be retried.")]
        internal static partial void GraphQueryServiceError(this ILogger logger, Exception exception);
    }
}