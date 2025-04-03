/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Threading;
using System.Threading.Tasks;

namespace GraphlessDB.Graph.Services.Internal
{
    internal interface IGraphQueryService
    {
        Task ClearAsync(CancellationToken cancellationToken);

        Task<TryGetNodesResponse> TryGetNodesAsync(
            TryGetNodesRequest request,
            CancellationToken cancellationToken);

        Task<TryGetVersionedNodesResponse> TryGetVersionedNodesAsync(
            TryGetVersionedNodesRequest request,
            CancellationToken cancellationToken);

        Task<TryGetEdgesResponse> TryGetEdgesAsync(
            TryGetEdgesRequest request,
            CancellationToken cancellationToken);

        Task<GetConnectionResponse> GetConnectionByTypeAsync(
            GetConnectionByTypeRequest request,
            CancellationToken cancellationToken);

        Task<GetConnectionResponse> GetConnectionByTypeAndPropertyNameAsync(
            GetConnectionByTypeAndPropertyNameRequest request,
            CancellationToken cancellationToken);

        Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValueAsync(
            GetConnectionByTypePropertyNameAndValueRequest request,
            CancellationToken cancellationToken);

        Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValuesAsync(
            GetConnectionByTypePropertyNameAndValuesRequest request,
            CancellationToken cancellationToken);

        Task<ToEdgeQueryResponse> GetInToEdgeConnectionAsync(
            ToEdgeQueryRequest request,
            CancellationToken cancellationToken);

        Task<ToEdgeQueryResponse> GetOutToEdgeConnectionAsync(
            ToEdgeQueryRequest request,
            CancellationToken cancellationToken);

        Task<ToEdgeQueryResponse> GetInAndOutToEdgeConnectionAsync(
            ToEdgeQueryRequest request,
            CancellationToken cancellationToken);

        Task PutAsync(
            PutRequest request,
            CancellationToken cancellationToken);
    }
}