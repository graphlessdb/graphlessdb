/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB.Threading
{
    public sealed class Retry
    {
        public static async Task<T> RunAsync<T>(Func<Task<T>> func, RetryOptions options, CancellationToken cancellationToken)
        {
            var retryAttempt = 0;
            while (true)
            {
                try
                {
                    return await func();
                }
                catch (Exception ex)
                {
                    var retryInfo = options.Condition(new RetryConditionRequest(retryAttempt, ex, cancellationToken));
                    if (!retryInfo.Retry)
                    {
                        throw;
                    }

                    retryAttempt++;
                    await Task.Delay(retryInfo.RetryDelay, cancellationToken);
                }
            }
        }

        public static async Task RunAsync(Func<Task> func, RetryOptions options, CancellationToken cancellationToken)
        {
            var retryAttempt = 0;
            while (true)
            {
                try
                {
                    await func();
                    return;
                }
                catch (Exception ex)
                {
                    var retryInfo = options.Condition(new RetryConditionRequest(retryAttempt, ex, cancellationToken));
                    if (!retryInfo.Retry)
                    {
                        throw;
                    }

                    retryAttempt++;
                    await Task.Delay(retryInfo.RetryDelay, cancellationToken);
                }
            }
        }
    }
}
