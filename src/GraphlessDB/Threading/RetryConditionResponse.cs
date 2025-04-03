/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;

namespace GraphlessDB.Threading
{
    public sealed record RetryConditionResponse(bool Retry, TimeSpan RetryDelay)
    {
        public static readonly RetryConditionResponse Default = new(true, TimeSpan.Zero);
    }
}
