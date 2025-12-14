/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using GraphlessDB.Domain;
using GraphlessDB.Domain.Graph;
using GraphlessDB.Domain.Services;

namespace GraphlessDB
{
    public sealed class RequestSizeLimitExceededException(string? message = null, Exception? innerException = null) : GraphlessDBException(message, innerException);
}
