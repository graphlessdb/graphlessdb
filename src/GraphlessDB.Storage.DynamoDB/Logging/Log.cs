/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using Microsoft.Extensions.Logging;

namespace GraphlessDB.Logging
{
    internal static partial class Log
    {
        [LoggerMessage(EventId = 1, Level = LogLevel.Warning, Message = "Entity in transaction caused cancellation.  CancellationReasonCode={CancellationReasonCode} Item={Item} ConditionExpression={ConditionExpression} ExpressionAttributeNames={ExpressionAttributeNames} ExpressionAttributeValues={ExpressionAttributeValues}")]
        internal static partial void EntityInTransactionCausedCancellation(this ILogger logger, string cancellationReasonCode, string item, string conditionExpression, string expressionAttributeNames, string expressionAttributeValues);
    }
}