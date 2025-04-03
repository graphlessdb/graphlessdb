/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class GraphEdgeFilterService(
        IGraphNodeFilterService nodeFilterService,
        IGraphQueryService graphDataQueryService,
        IGraphQueryablePropertyService queryablePropertyService) : IGraphEdgeFilterService
    {
        public bool IsPostFilteringRequired(IEdgeFilter? filter)
        {
            return filter != null;
        }

        public async Task<bool> IsFilterMatchAsync(
            IEdge edge,
            IEdgeFilter? filter,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            var oldFilter = filter?.GetEdgeFilter();
            if (oldFilter?.NodeInFilter != null)
            {
                var nodeIn = await graphDataQueryService.GetNodeAsync(edge.InId, consistentRead, cancellationToken);
                if (!await nodeFilterService.IsFilterMatchAsync(nodeIn.Node, oldFilter.NodeInFilter, consistentRead, cancellationToken))
                {
                    return false;
                }
            }

            if (oldFilter?.NodeOutFilter != null)
            {
                var nodeOut = await graphDataQueryService.GetNodeAsync(edge.OutId, consistentRead, cancellationToken);
                if (!await nodeFilterService.IsFilterMatchAsync(nodeOut.Node, oldFilter.NodeOutFilter, consistentRead, cancellationToken))
                {
                    return false;
                }
            }

            // TODO
            // if (f.ValueFilterItems.Any())
            // {
            //     var isMatch = await IsNodeValueFilterMatchAsync(node, f, consistentRead, cancellationToken);
            //     if (!isMatch)
            //     {
            //         return false;
            //     }
            // }

            return true;
        }

        public EdgePushdownQueryData? TryGetEdgePushdownQueryData(
            string? edgeTypeName,
            IEdgeFilter? filter,
            IEdgeOrder? order)
        {
            var orderArguments = TryGetOrderArguments(order);
            var filterArguments = TryGetFilterArguments(edgeTypeName, filter, order);
            return new EdgePushdownQueryData(orderArguments, filterArguments);
        }

        private EdgeFilterArguments? TryGetFilterArguments(string? edgeTypeName, IEdgeFilter? filter, IEdgeOrder? order)
        {
            if (filter == null)
            {
                return null;
            }

            var filterOld = filter.GetEdgeFilter();

            if (filterOld == null || filterOld.ValueFilterItems.IsEmpty)
            {
                return null;
            }

            // If an order is specified then we must either
            // - have no filter pushed down the to db
            // - have a filter which also matches the order arg which can be shared and pushed down to the db
            // - CANNOT have a order A and a filter B
            var orderArguments = TryGetOrderArguments(order);
            if (orderArguments != null)
            {
                var matchingValueFilter = filterOld
                    .ValueFilterItems
                    .Where(v => v.Name == orderArguments.PropertyName)
                    .SingleOrDefault();

                if (matchingValueFilter != null)
                {
                    return TryGetFilterArguments(edgeTypeName ?? throw new NotSupportedException(), matchingValueFilter);
                }

                // No match so we cant use any of the filters to push down to db
                return null;
            }

            return filterOld
                .ValueFilterItems
                .Select(f => TryGetFilterArguments(edgeTypeName ?? throw new NotSupportedException(), f))
                .FirstOrDefault();
        }

        private static OrderArguments? TryGetOrderArguments(IEdgeOrder? order)
        {
            if (order == null)
            {
                return null;
            }

            var orderOld = AsOrder(order);
            if (orderOld.Item == null)
            {
                return null;
            }

            return new OrderArguments(orderOld.Item.Name, orderOld.Item.Value);
        }

        private EdgeFilterArguments? TryGetFilterArguments(string typeName, ValueFilterItem valueFilter)
        {
            // Check if this propert is indexed in the db, if not then we cant push this down to the db level
            if (!queryablePropertyService.IsQueryableProperty(typeName, valueFilter.Name))
            {
                return null;
            }

            return valueFilter.Value switch
            {
                DateTimeFilter typedFilter => TryGetFilterArguments(valueFilter.Name, typedFilter),
                EnumFilter typedFilter => TryGetFilterArguments(valueFilter.Name, typedFilter),
                IdFilter typedFilter => TryGetFilterArguments(valueFilter.Name, typedFilter),
                StringFilter typedFilter => TryGetFilterArguments(valueFilter.Name, typedFilter),
                IntFilter typedFilter => TryGetFilterArguments(valueFilter.Name, typedFilter),
                _ => throw new NotSupportedException(),
            };
        }

#pragma warning disable IDE0060 // Remove unused parameter
        private static EdgeFilterArguments? TryGetFilterArguments(string name, DateTimeFilter filter)
        {
            // NotImplemented
            return null;
        }

        private static EdgeFilterArguments? TryGetFilterArguments(string name, EnumFilter filter)
        {
            // NotImplemented
            return null;
        }

        private static EdgeFilterArguments? TryGetFilterArguments(string name, IdFilter filter)
        {
            // NotImplemented
            return null;
        }
#pragma warning restore IDE0060 // Remove unused parameter

        private static EdgeFilterArguments? TryGetFilterArguments(string name, StringFilter filter)
        {
            if (filter.Eq != null)
            {
                return new EdgeFilterArguments(name, PropertyOperator.Equals, filter.Eq);
            }

            if (filter.BeginsWith != null)
            {
                return new EdgeFilterArguments(name, PropertyOperator.StartsWith, filter.BeginsWith);
            }

            return null;
        }

        private static EdgeFilterArguments? TryGetFilterArguments(string name, IntFilter filter)
        {
            throw new NotSupportedException("IntFilter not supported");
        }

        [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2075:DynamicallyAccessedMembersAttribute")]
        private static NodeOrder AsOrder(IEdgeOrder? source)
        {
            if (source == null)
            {
                return NodeOrder.Empty;
            }

            return new NodeOrder(source
                .GetType()
                .GetProperties()
                .Select(p => new { p.Name, Value = (OrderDirection?)p.GetValue(source) })
                .Where(v => v.Value != null)
                .Select(v => new OrderItem(v.Name, v.Value ?? throw new GraphlessDBOperationException("Expected value")))
                .SingleOrDefault());
        }
    }
}
