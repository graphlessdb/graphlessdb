/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Threading;

namespace GraphlessDB.Threading
{
    public sealed record RetryConditionRequest(int RetryAttempt, Exception Exception, CancellationToken CancellationToken);
}
