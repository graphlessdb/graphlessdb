/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;
using GraphlessDB.Linq;

namespace GraphlessDB.Query.Services.Internal
{
    internal sealed class GraphNodeFilterService(
        IGraphNodeFilterDataLayerService nodeFilterDataLayerService,
        IGraphQueryService graphDataQueryService,
        IGraphCursorSerializationService cursorSerializer,
        IGraphQueryablePropertyService queryablePropertyService,
        IGraphSerializationService entityValueSerializer) : IGraphNodeFilterService
    {
        public bool IsPostFilteringRequired(INodeFilter? filter)
        {
            return filter != null;
        }


        public NodePushdownQueryData? TryGetNodePushdownQueryData(
            string type,
            INodeFilter? filter,
            INodeOrder? order,
            CancellationToken cancellationToken)
        {
            filter = nodeFilterDataLayerService.TryGetDataLayerFilter(filter, cancellationToken);
            var filterOld = AsEntityFilter(filter);
            var orderOld = AsOrder(order);
            var dataLayerOrder = TryGetDataLayerNodeOrder(type, filterOld, orderOld);
            if (dataLayerOrder == null)
            {
                return null;
            }

            var dataLayerFilter = TryGetDataLayerNodeFilter(dataLayerOrder, filterOld);
            return new NodePushdownQueryData(dataLayerOrder, dataLayerFilter);
        }

        public async Task<bool> IsFilterMatchAsync(
            INode node,
            INodeFilter? filter,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            // TODO
            var filterOld = AsEntityFilter(filter);

            var isValueMatch = filterOld
                .ValueFilterItems
                .All(filterItem => IsFilterMatch(node, filterItem));

            if (!isValueMatch)
            {
                return false;
            }

            var edgeMatches = await Task.WhenAll(filterOld.EdgeFilterItems.Select(async f =>
            {
                if (f.NodeInFilter != null)
                {
                    var isMatch = await IsNodeInFilterMatchAsync(node, f, consistentRead, cancellationToken);
                    if (!isMatch)
                    {
                        return false;
                    }
                }

                if (f.NodeOutFilter != null)
                {
                    var isMatch = await IsNodeOutFilterMatchAsync(node, f, consistentRead, cancellationToken);
                    if (!isMatch)
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
            }));

            if (edgeMatches.Any(m => !m))
            {
                return false;
            }

            if (filter != null && await nodeFilterDataLayerService.IsNodeExcludedAsync(node, filter, cancellationToken))
            {
                return false;
            }

            return true;
        }

        private OrderArguments? TryGetDataLayerNodeOrder(string type, NodeFilter filter, NodeOrder order)
        {
            // Use the order first
            if (order.Item != null)
            {
                if (!queryablePropertyService.IsQueryableProperty(type, order.Item.Name))
                {
                    throw new GraphlessDBOperationException("Cannot order using this property");
                }

                return new OrderArguments(order.Item.Name, order.Item.Value);
            }

            // If there is no order then use a filter if available
            var queryableFilterItem = filter
                .ValueFilterItems
                .Where(v => IsFilterPushdownSupported(type, v))
                .FirstOrDefault();

            return queryableFilterItem != null
                ? new OrderArguments(queryableFilterItem.Name, OrderDirection.Asc)
                : null;
        }

        private NodeFilterArguments? TryGetDataLayerNodeFilter(OrderArguments order, NodeFilter filter)
        {
            // Return a matching filter if there is one and if the operator and value are supported at the database level
            var matchingValueFilter = filter
                .ValueFilterItems
                .Where(v => v.Name == order.PropertyName && TryGetPropertyOperator(v.Value) != null)
                .FirstOrDefault();

            if (matchingValueFilter == null)
            {
                return null;
            }

            var propertyOperator = TryGetPropertyOperator(matchingValueFilter.Value) ?? throw new GraphlessDBOperationException("Expected property operator");
            var propertyValues = GetPropertyValues(matchingValueFilter.Value);
            return new NodeFilterArguments(propertyOperator, propertyValues);
        }

        public ImmutableList<string> GetPropertyValues(IValueFilter value)
        {
            return value switch
            {
                DateTimeFilter filter => GetPropertyValues(filter),
                EnumFilter filter => GetPropertyValues(filter),
                IdFilter filter => GetPropertyValues(filter),
                StringFilter filter => GetPropertyValues(filter),
                _ => throw new NotSupportedException(),
            };
        }

        private ImmutableList<string> GetPropertyValues(DateTimeFilter filter)
        {
            if (filter.Eq != null)
            {
                return [entityValueSerializer.GetPropertyAsString(filter.Eq)];
            }

            return [];
        }

        private ImmutableList<string> GetPropertyValues(EnumFilter filter)
        {
            if (filter.Eq != null)
            {
                return [entityValueSerializer.GetPropertyAsString(filter.Eq)];
            }

            return [];
        }

        private ImmutableList<string> GetPropertyValues(IdFilter filter)
        {
            if (filter.Eq != null)
            {
                return [entityValueSerializer.GetPropertyAsString(filter.Eq)];
            }

            return [];
        }

        private ImmutableList<string> GetPropertyValues(StringFilter filter)
        {
            if (filter.Eq != null)
            {
                return [entityValueSerializer.GetPropertyAsString(filter.Eq)];
            }

            if (filter.BeginsWith != null)
            {
                return [entityValueSerializer.GetPropertyAsString(filter.BeginsWith)];
            }

            if (filter.BeginsWithAny != null)
            {
                return filter
                    .BeginsWithAny
                    .Select(entityValueSerializer.GetPropertyAsString)
                    .ToImmutableList();
            }

            return [];
        }

        private static PropertyOperator? TryGetPropertyOperator(IValueFilter value)
        {
            return value switch
            {
                DateTimeFilter filter => TryGetPropertyOperator(filter),
                EnumFilter filter => TryGetPropertyOperator(filter),
                IdFilter filter => TryGetPropertyOperator(filter),
                StringFilter filter => TryGetPropertyOperator(filter),
                _ => throw new NotSupportedException(),
            };
        }

        private static PropertyOperator? TryGetPropertyOperator(DateTimeFilter filter)
        {
            if (filter.Eq != null)
            {
                return PropertyOperator.Equals;
            }

            return null;
        }

        private static PropertyOperator? TryGetPropertyOperator(EnumFilter filter)
        {
            if (filter.Eq != null)
            {
                return PropertyOperator.Equals;
            }

            return null;
        }

        private static PropertyOperator? TryGetPropertyOperator(IdFilter filter)
        {
            if (filter.Eq != null)
            {
                return PropertyOperator.Equals;
            }

            return null;
        }

        private static PropertyOperator? TryGetPropertyOperator(StringFilter filter)
        {
            if (filter.Eq != null)
            {
                return PropertyOperator.Equals;
            }

            if (filter.BeginsWith != null)
            {
                return PropertyOperator.StartsWith;
            }

            if (filter.BeginsWithAny != null)
            {
                return PropertyOperator.StartsWith;
            }

            return null;
        }

        private bool IsFilterPushdownSupported(string typeName, ValueFilterItem valueFilterItem)
        {
            if (!queryablePropertyService.IsQueryableProperty(typeName, valueFilterItem.Name))
            {
                return false;
            }

            if (TryGetPropertyOperator(valueFilterItem.Value) == null)
            {
                return false;
            }

            return true;
        }

        private async Task<bool> AllAreFilterMatchAsync(
            Connection<RelayEdge<INode>, INode> connection,
            INodeFilter filter,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            if (connection.Edges.IsEmpty)
            {
                throw new ArgumentException("Expected non empty connection", nameof(connection));
            }

            // Filter using edges nexts
            var responses = await Task.WhenAll(connection
                .Edges
                .Select(edge => IsFilterMatchAsync(edge.Node, filter, consistentRead, cancellationToken)));

            return responses.All(r => r);
        }

        private async Task<bool> IsNodeInFilterMatchAsync(
            INode node,
            EdgeFilter edgeFilterItem,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            if (edgeFilterItem.NodeInFilter == null)
            {
                throw new ArgumentException("Expected NodeOutFilter to not be null", nameof(edgeFilterItem));
            }

            var toEdgeRequest = new ToEdgeQueryRequest(
                 edgeFilterItem.NodeOutTypeName,
                 edgeFilterItem.EdgeTypeName,
                 new Connection<RelayEdge<INode>, INode>([new RelayEdge<INode>(GetNodeByIdCursor(node), node)], new PageInfo(false, false, node.Id, node.Id)),
                 null,
                 null,
                 ConnectionArguments.Default,
                 consistentRead);

            var toEdgeResponse = await graphDataQueryService.GetOutToEdgeConnectionAsync(toEdgeRequest, cancellationToken);

            var getNodesRequest = new GetNodesRequest(
                toEdgeResponse.Connection.Edges.Select(e => e.Node.InId).ToImmutableList(),
                consistentRead);

            var getNodesResponse = await graphDataQueryService.GetNodesAsync(getNodesRequest, cancellationToken);

            var connection = new Connection<RelayEdge<INode>, INode>(
                getNodesResponse.Nodes,
                new PageInfo(false, false, getNodesResponse.Nodes.TryGetStartCursor() ?? string.Empty, getNodesResponse.Nodes.TryGetEndCursor() ?? string.Empty));

            if (connection.Edges.IsEmpty)
            {
                // No edges were found so nothing can match the filter
                return false;
            }

            return await AllAreFilterMatchAsync(connection, edgeFilterItem.NodeInFilter, consistentRead, cancellationToken);
        }

        private async Task<bool> IsNodeOutFilterMatchAsync(
            INode node,
            EdgeFilter edgeFilterItem,
            bool consistentRead,
            CancellationToken cancellationToken)
        {
            if (edgeFilterItem.NodeOutFilter == null)
            {
                throw new ArgumentException("Expected NodeOutFilter to not be null", nameof(edgeFilterItem));
            }

            var toEdgeRequest = new ToEdgeQueryRequest(
                edgeFilterItem.NodeInTypeName,
                edgeFilterItem.EdgeTypeName,
                new Connection<RelayEdge<INode>, INode>([new RelayEdge<INode>(GetNodeByIdCursor(node), node)], new PageInfo(false, false, node.Id, node.Id)),
                null,
                null,
                ConnectionArguments.Default,
                consistentRead);

            var toEdgeResponse = await graphDataQueryService.GetInToEdgeConnectionAsync(toEdgeRequest, cancellationToken);

            var getNodesRequest = new GetNodesRequest(
                toEdgeResponse.Connection.Edges.Select(e => e.Node.OutId).ToImmutableList(),
                consistentRead);

            var getNodesResponse = await graphDataQueryService.GetNodesAsync(getNodesRequest, cancellationToken);

            var connection = new Connection<RelayEdge<INode>, INode>(
                getNodesResponse.Nodes,
                new PageInfo(false, false, getNodesResponse.Nodes.TryGetStartCursor() ?? string.Empty, getNodesResponse.Nodes.TryGetEndCursor() ?? string.Empty));

            if (connection.Edges.IsEmpty)
            {
                // No edges were found so nothing can match the filter
                return false;
            }

            return await AllAreFilterMatchAsync(connection, edgeFilterItem.NodeOutFilter, consistentRead, cancellationToken);
        }

        private string GetNodeByIdCursor(INode node)
        {
            // TODO Check partition logic here
            var cursor = Cursor.Create(CursorNode.Empty with { HasType = new HasTypeCursor(node.Id, "0", []) });
            return cursorSerializer.Serialize(cursor);
        }

        [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2075:DynamicallyAccessedMembersAttribute")]
        private static bool IsFilterMatch<TNode>(TNode node, ValueFilterItem filterItem)
            where TNode : class
        {
            var property = node.GetType().GetProperty(filterItem.Name)
                ?? throw new GraphlessDBOperationException($"Expected property '{filterItem.Name}' was missing");

            var propertyValue = property.GetValue(node);
            return filterItem.Value switch
            {
                IdFilter idFilter => propertyValue switch
                {
                    string stringValue => idFilter.IsMatch(stringValue),
                    null => false, // TODO This isnt very nice as we cant match on null
                    _ => throw new NotSupportedException(),
                },
                StringFilter stringFilter => propertyValue switch
                {
                    string stringValue => stringFilter.IsMatch(stringValue),
                    null => false, // TODO This isnt very nice as we cant match on null
                    _ => throw new NotSupportedException(),
                },
                DateTimeFilter dateTimeFilter => propertyValue switch
                {
                    DateTime dateTimeValue => dateTimeFilter.IsMatch(dateTimeValue),
                    null => false,
                    _ => throw new NotSupportedException(),
                },
                EnumFilter enumFilter => enumFilter.IsMatch(propertyValue),
                _ => throw new NotSupportedException(),
            };
        }

        // private static Connection<RelayEdge<INode>, INode> AsConnection(INode node)
        // {
        //     var cursor = CursorSerializer.Serialize(new Cursor(ImmutableList.Create(new CursorItem(new HasTypeCursor(node.Id), null, null, null, null, null))));
        //     return new Connection<RelayEdge<INode>, INode>(
        //         ImmutableList.Create(new RelayEdge<INode>(cursor, node)),
        //         new PageInfo(false, false, cursor, cursor));
        // }

        [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2075:DynamicallyAccessedMembersAttribute")]
        public static NodeFilter AsEntityFilter(INodeFilter? source)
        {
            if (source == null)
            {
                return NodeFilter.Empty;
            }

            var valueFilterItems = source
                .GetType()
                .GetProperties()
                .Where(p => p.GetMethod != null && typeof(IValueFilter).IsAssignableFrom(p.GetMethod.ReturnType))
                .Select(p => new
                {
                    p.Name,
                    Value = (IValueFilter?)p.GetValue(source)
                })
                .Where(v => v.Value != null)
                .Select(v => new ValueFilterItem(v.Name, v.Value ?? throw new GraphlessDBOperationException("Expected value")))
                .ToImmutableList();

            var otherValueFilterItems = source
                .GetType()
                .GetProperties()
                .Where(p => p.GetMethod != null && typeof(IHasValueFilter).IsAssignableFrom(p.GetMethod.ReturnType))
                .Select(p => new
                {
                    p.Name,
                    Value = (IHasValueFilter?)p.GetValue(source)
                })
                .Where(v => v.Value != null)
                .Select(v => new ValueFilterItem(v.Name, v.Value?.GetValueFilter() ?? throw new GraphlessDBOperationException("Expected value filter")))
                .ToImmutableList();

            var edgeFilterItems = source
                .GetType()
                .GetProperties()
                .Where(p => p.GetMethod != null && typeof(IEdgeFilter).IsAssignableFrom(p.GetMethod.ReturnType))
                .Select(p => new
                {
                    p.Name,
                    Value = (IEdgeFilter?)p.GetValue(source)
                })
                .Select(v => v.Value?.GetEdgeFilter())
                .WhereNotNull()
                .ToImmutableList();

            // if (source is IEdgeFilter edgeFilter)
            // {
            //     return new EntityFilter(
            //         valueFilterItems.AddRange(otherValueFilterItems),
            //         edgeFilter.GetNodeInFilter(),
            //         edgeFilter.GetNodeOutFilter());
            // }

            return new NodeFilter(
                valueFilterItems.AddRange(otherValueFilterItems),
                edgeFilterItems);
        }

        [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2075:DynamicallyAccessedMembersAttribute")]
        private static NodeOrder AsOrder(INodeOrder? source)
        {
            if (source == null)
            {
                return NodeOrder.Empty;
            }

            // if (source is EntityOrder entityOrder)
            // {
            //     return entityOrder;
            // }

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