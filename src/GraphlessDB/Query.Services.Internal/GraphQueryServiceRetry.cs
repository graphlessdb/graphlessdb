/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Threading;

namespace GraphlessDB.Query.Services.Internal
{
    internal static class GraphQueryServiceRetry
    {
        public static readonly RetryOptions RetryOptions = new()
        {
            Condition = v => new RetryConditionResponse(GetRetry(v), GetRetryDelay(v))
        };

        private static bool GetRetry(RetryConditionRequest r)
        {
            return r.Exception is EdgesNotFoundException && !r.CancellationToken.IsCancellationRequested;
        }

        private static TimeSpan GetRetryDelay(RetryConditionRequest r)
        {
            return TimeSpan.FromSeconds(1 + r.RetryAttempt * 1 + Math.Pow(r.RetryAttempt * 1, 2.0));
        }
    }
}
