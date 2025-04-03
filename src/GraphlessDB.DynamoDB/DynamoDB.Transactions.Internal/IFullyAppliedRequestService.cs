/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB.DynamoDB.Transactions.Internal
{
    public interface IFullyAppliedRequestService
    {
        Task SetFullyAppliedAsync(TransactionVersion key, CancellationToken cancellationToken);

        Task<bool> IsFullyAppliedAsync(TransactionVersion key, CancellationToken cancellationToken);
    }
}