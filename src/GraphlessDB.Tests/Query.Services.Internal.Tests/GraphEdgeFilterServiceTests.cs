/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using GraphlessDB.Graph;
using GraphlessDB.Graph.Services;
using GraphlessDB.Graph.Services.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Query.Services.Internal.Tests
{
    [TestClass]
    public sealed class GraphEdgeFilterServiceTests
    {
        private static GraphEdgeFilterService CreateService(
            IGraphNodeFilterService? nodeFilterService = null,
            IGraphQueryService? graphQueryService = null,
            IGraphQueryablePropertyService? queryablePropertyService = null)
        {
            return new GraphEdgeFilterService(
                nodeFilterService ?? new MockGraphNodeFilterService(),
                graphQueryService ?? new MockGraphQueryService(),
                queryablePropertyService ?? new MockQueryablePropertyService());
        }

        [TestMethod]
        public void IsPostFilteringRequiredWithNullFilterReturnsFalse()
        {
            var service = CreateService();
            var result = service.IsPostFilteringRequired(null);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void IsPostFilteringRequiredWithFilterReturnsTrue()
        {
            var service = CreateService();
            var filter = new TestEdgeFilter();
            var result = service.IsPostFilteringRequired(filter);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNullFilterReturnsTrue()
        {
            var service = CreateService();
            var edge = CreateTestEdge();
            var result = await service.IsFilterMatchAsync(edge, null, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithEmptyEdgeFilterReturnsTrue()
        {
            var service = CreateService();
            var edge = CreateTestEdge();
            var filter = new TestEdgeFilter { };
            var result = await service.IsFilterMatchAsync(edge, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNodeInFilterMatchReturnsTrue()
        {
            var nodeFilterService = new MockGraphNodeFilterService(true);
            var graphQueryService = new MockGraphQueryService();
            var service = CreateService(nodeFilterService, graphQueryService);
            var edge = CreateTestEdge();
            var filter = new TestEdgeFilterWithNodeIn();
            var result = await service.IsFilterMatchAsync(edge, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNodeInFilterNoMatchReturnsFalse()
        {
            var nodeFilterService = new MockGraphNodeFilterService(false);
            var graphQueryService = new MockGraphQueryService();
            var service = CreateService(nodeFilterService, graphQueryService);
            var edge = CreateTestEdge();
            var filter = new TestEdgeFilterWithNodeIn();
            var result = await service.IsFilterMatchAsync(edge, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNodeOutFilterMatchReturnsTrue()
        {
            var nodeFilterService = new MockGraphNodeFilterService(true);
            var graphQueryService = new MockGraphQueryService();
            var service = CreateService(nodeFilterService, graphQueryService);
            var edge = CreateTestEdge();
            var filter = new TestEdgeFilterWithNodeOut();
            var result = await service.IsFilterMatchAsync(edge, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNodeOutFilterNoMatchReturnsFalse()
        {
            var nodeFilterService = new MockGraphNodeFilterService(false);
            var graphQueryService = new MockGraphQueryService();
            var service = CreateService(nodeFilterService, graphQueryService);
            var edge = CreateTestEdge();
            var filter = new TestEdgeFilterWithNodeOut();
            var result = await service.IsFilterMatchAsync(edge, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithBothNodeFiltersMatchReturnsTrue()
        {
            var nodeFilterService = new MockGraphNodeFilterService(true);
            var graphQueryService = new MockGraphQueryService();
            var service = CreateService(nodeFilterService, graphQueryService);
            var edge = CreateTestEdge();
            var filter = new TestEdgeFilterWithBothNodes();
            var result = await service.IsFilterMatchAsync(edge, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNodeInMatchAndNodeOutNoMatchReturnsFalse()
        {
            var callCount = 0;
            var nodeFilterService = new MockGraphNodeFilterService(() =>
            {
                callCount++;
                return callCount == 1; // First call (NodeIn) returns true, second (NodeOut) returns false
            });
            var graphQueryService = new MockGraphQueryService();
            var service = CreateService(nodeFilterService, graphQueryService);
            var edge = CreateTestEdge();
            var filter = new TestEdgeFilterWithBothNodes();
            var result = await service.IsFilterMatchAsync(edge, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithNullFilterReturnsData()
        {
            var service = CreateService();
            var result = service.TryGetEdgePushdownQueryData(null, null, null);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Order);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithEmptyFilterReturnsNullFilter()
        {
            var service = CreateService();
            var filter = new TestEdgeFilter();
            var result = service.TryGetEdgePushdownQueryData("TestEdge", filter, null);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithOrderReturnsOrderArguments()
        {
            var service = CreateService();
            var order = new TestEdgeOrder { Name = OrderDirection.Asc };
            var result = service.TryGetEdgePushdownQueryData(null, null, order);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.AreEqual("Name", result.Order.PropertyName);
            Assert.AreEqual(OrderDirection.Asc, result.Order.Direction);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithEmptyOrderReturnsNullOrder()
        {
            var service = CreateService();
            var order = new TestEdgeOrder(); // No properties set
            var result = service.TryGetEdgePushdownQueryData(null, null, order);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Order);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithStringFilterEqReturnsFilterArguments()
        {
            var queryablePropertyService = new MockQueryablePropertyService(true);
            var service = CreateService(queryablePropertyService: queryablePropertyService);
            var filter = new TestEdgeFilterWithStringProperty { Name = new StringFilter { Eq = "test" } };
            var result = service.TryGetEdgePushdownQueryData("TestEdge", filter, null);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Filter);
            Assert.AreEqual("Name", result.Filter.PropertyName);
            Assert.AreEqual(PropertyOperator.Equals, result.Filter.PropertyOperator);
            Assert.AreEqual("test", result.Filter.PropertyValue);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithStringFilterBeginsWithReturnsFilterArguments()
        {
            var queryablePropertyService = new MockQueryablePropertyService(true);
            var service = CreateService(queryablePropertyService: queryablePropertyService);
            var filter = new TestEdgeFilterWithStringProperty { Name = new StringFilter { BeginsWith = "prefix" } };
            var result = service.TryGetEdgePushdownQueryData("TestEdge", filter, null);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Filter);
            Assert.AreEqual("Name", result.Filter.PropertyName);
            Assert.AreEqual(PropertyOperator.StartsWith, result.Filter.PropertyOperator);
            Assert.AreEqual("prefix", result.Filter.PropertyValue);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithStringFilterNoEqOrBeginsWithReturnsNullFilter()
        {
            var queryablePropertyService = new MockQueryablePropertyService(true);
            var service = CreateService(queryablePropertyService: queryablePropertyService);
            var filter = new TestEdgeFilterWithStringProperty { Name = new StringFilter { Ne = "test" } };
            var result = service.TryGetEdgePushdownQueryData("TestEdge", filter, null);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithNonQueryablePropertyReturnsNullFilter()
        {
            var queryablePropertyService = new MockQueryablePropertyService(false);
            var service = CreateService(queryablePropertyService: queryablePropertyService);
            var filter = new TestEdgeFilterWithStringProperty { Name = new StringFilter { Eq = "test" } };
            var result = service.TryGetEdgePushdownQueryData("TestEdge", filter, null);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithOrderAndMatchingFilterReturnsFilterArguments()
        {
            var queryablePropertyService = new MockQueryablePropertyService(true);
            var service = CreateService(queryablePropertyService: queryablePropertyService);
            var order = new TestEdgeOrder { Name = OrderDirection.Asc };
            var filter = new TestEdgeFilterWithStringProperty { Name = new StringFilter { Eq = "test" } };
            var result = service.TryGetEdgePushdownQueryData("TestEdge", filter, order);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.IsNotNull(result.Filter);
            Assert.AreEqual("Name", result.Filter.PropertyName);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithOrderAndNonMatchingFilterReturnsNullFilter()
        {
            var queryablePropertyService = new MockQueryablePropertyService(true);
            var service = CreateService(queryablePropertyService: queryablePropertyService);
            var order = new TestEdgeOrder { Name = OrderDirection.Asc };
            var filter = new TestEdgeFilterWithStringProperty { Status = new StringFilter { Eq = "test" } };
            var result = service.TryGetEdgePushdownQueryData("TestEdge", filter, order);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithMultipleFiltersReturnsFirstFilter()
        {
            var queryablePropertyService = new MockQueryablePropertyService(true);
            var service = CreateService(queryablePropertyService: queryablePropertyService);
            var filter = new TestEdgeFilterWithMultipleProperties
            {
                Name = new StringFilter { Eq = "first" },
                Status = new StringFilter { Eq = "second" }
            };
            var result = service.TryGetEdgePushdownQueryData("TestEdge", filter, null);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Filter);
            // Should return the first queryable filter found
            Assert.IsTrue(result.Filter.PropertyName == "Name" || result.Filter.PropertyName == "Status");
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithDateTimeFilterReturnsNull()
        {
            var queryablePropertyService = new MockQueryablePropertyService(true);
            var service = CreateService(queryablePropertyService: queryablePropertyService);
            var filter = new TestEdgeFilterWithDateTimeProperty { CreatedAt = new DateTimeFilter { Eq = DateTime.UtcNow } };
            var result = service.TryGetEdgePushdownQueryData("TestEdge", filter, null);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithEnumFilterReturnsNull()
        {
            var queryablePropertyService = new MockQueryablePropertyService(true);
            var service = CreateService(queryablePropertyService: queryablePropertyService);
            var filter = new TestEdgeFilterWithEnumProperty { Status = new EnumFilter { Eq = TestEnum.Value1 } };
            var result = service.TryGetEdgePushdownQueryData("TestEdge", filter, null);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithIdFilterReturnsNull()
        {
            var queryablePropertyService = new MockQueryablePropertyService(true);
            var service = CreateService(queryablePropertyService: queryablePropertyService);
            var filter = new TestEdgeFilterWithIdProperty { Id = new IdFilter { Eq = "test-id" } };
            var result = service.TryGetEdgePushdownQueryData("TestEdge", filter, null);
            Assert.IsNotNull(result);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithIntFilterThrowsNotSupportedException()
        {
            var queryablePropertyService = new MockQueryablePropertyService(true);
            var service = CreateService(queryablePropertyService: queryablePropertyService);
            var filter = new TestEdgeFilterWithIntProperty { Count = new IntFilter { Eq = 5 } };
            Assert.ThrowsException<NotSupportedException>(() =>
                service.TryGetEdgePushdownQueryData("TestEdge", filter, null));
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithNullEdgeTypeNameThrowsNotSupportedException()
        {
            var queryablePropertyService = new MockQueryablePropertyService(true);
            var service = CreateService(queryablePropertyService: queryablePropertyService);
            var filter = new TestEdgeFilterWithStringProperty { Name = new StringFilter { Eq = "test" } };
            Assert.ThrowsException<NotSupportedException>(() =>
                service.TryGetEdgePushdownQueryData(null, filter, null));
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithOrderAndNullEdgeTypeNameReturnsOrderOnly()
        {
            var service = CreateService();
            var order = new TestEdgeOrder { Name = OrderDirection.Asc };
            var result = service.TryGetEdgePushdownQueryData(null, null, order);
            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetEdgePushdownQueryDataWithUnsupportedFilterThrowsNotSupportedException()
        {
            var queryablePropertyService = new MockQueryablePropertyService(true);
            var service = CreateService(queryablePropertyService: queryablePropertyService);
            var filter = new TestEdgeFilterWithUnsupportedProperty { Unsupported = new UnsupportedValueFilter() };
            Assert.ThrowsException<NotSupportedException>(() =>
                service.TryGetEdgePushdownQueryData("TestEdge", filter, null));
        }

        private static TestEdge CreateTestEdge()
        {
            var now = DateTime.UtcNow;
            return new TestEdge(now, now, DateTime.MinValue, "in-id", "out-id");
        }

        private sealed record TestEdge(DateTime CreatedAt, DateTime UpdatedAt, DateTime DeletedAt, string InId, string OutId)
            : IEdge(CreatedAt, UpdatedAt, DeletedAt, InId, OutId);

        private sealed class TestEdgeFilter : IEdgeFilter
        {
            public EdgeFilter? GetEdgeFilter()
            {
                return new EdgeFilter("TestEdge", "NodeIn", "NodeOut", null, null, ImmutableList<ValueFilterItem>.Empty);
            }
        }

        private sealed class TestEdgeFilterWithNodeIn : IEdgeFilter
        {
            public EdgeFilter? GetEdgeFilter()
            {
                return new EdgeFilter("TestEdge", "NodeIn", "NodeOut", new TestNodeFilter(), null, ImmutableList<ValueFilterItem>.Empty);
            }
        }

        private sealed class TestEdgeFilterWithNodeOut : IEdgeFilter
        {
            public EdgeFilter? GetEdgeFilter()
            {
                return new EdgeFilter("TestEdge", "NodeIn", "NodeOut", null, new TestNodeFilter(), ImmutableList<ValueFilterItem>.Empty);
            }
        }

        private sealed class TestEdgeFilterWithBothNodes : IEdgeFilter
        {
            public EdgeFilter? GetEdgeFilter()
            {
                return new EdgeFilter("TestEdge", "NodeIn", "NodeOut", new TestNodeFilter(), new TestNodeFilter(), ImmutableList<ValueFilterItem>.Empty);
            }
        }

        private sealed class TestEdgeFilterWithStringProperty : IEdgeFilter
        {
            public StringFilter? Name { get; set; }
            public StringFilter? Status { get; set; }

            public EdgeFilter? GetEdgeFilter()
            {
                var items = ImmutableList<ValueFilterItem>.Empty;
                if (Name != null)
                {
                    items = items.Add(new ValueFilterItem("Name", Name));
                }
                if (Status != null)
                {
                    items = items.Add(new ValueFilterItem("Status", Status));
                }
                return new EdgeFilter("TestEdge", "NodeIn", "NodeOut", null, null, items);
            }
        }

        private sealed class TestEdgeFilterWithMultipleProperties : IEdgeFilter
        {
            public StringFilter? Name { get; set; }
            public StringFilter? Status { get; set; }

            public EdgeFilter? GetEdgeFilter()
            {
                var items = ImmutableList<ValueFilterItem>.Empty;
                if (Name != null)
                {
                    items = items.Add(new ValueFilterItem("Name", Name));
                }
                if (Status != null)
                {
                    items = items.Add(new ValueFilterItem("Status", Status));
                }
                return new EdgeFilter("TestEdge", "NodeIn", "NodeOut", null, null, items);
            }
        }

        private sealed class TestEdgeFilterWithDateTimeProperty : IEdgeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }

            public EdgeFilter? GetEdgeFilter()
            {
                var items = ImmutableList<ValueFilterItem>.Empty;
                if (CreatedAt != null)
                {
                    items = items.Add(new ValueFilterItem("CreatedAt", CreatedAt));
                }
                return new EdgeFilter("TestEdge", "NodeIn", "NodeOut", null, null, items);
            }
        }

        private sealed class TestEdgeFilterWithEnumProperty : IEdgeFilter
        {
            public EnumFilter? Status { get; set; }

            public EdgeFilter? GetEdgeFilter()
            {
                var items = ImmutableList<ValueFilterItem>.Empty;
                if (Status != null)
                {
                    items = items.Add(new ValueFilterItem("Status", Status));
                }
                return new EdgeFilter("TestEdge", "NodeIn", "NodeOut", null, null, items);
            }
        }

        private sealed class TestEdgeFilterWithIdProperty : IEdgeFilter
        {
            public IdFilter? Id { get; set; }

            public EdgeFilter? GetEdgeFilter()
            {
                var items = ImmutableList<ValueFilterItem>.Empty;
                if (Id != null)
                {
                    items = items.Add(new ValueFilterItem("Id", Id));
                }
                return new EdgeFilter("TestEdge", "NodeIn", "NodeOut", null, null, items);
            }
        }

        private sealed class TestEdgeFilterWithIntProperty : IEdgeFilter
        {
            public IntFilter? Count { get; set; }

            public EdgeFilter? GetEdgeFilter()
            {
                var items = ImmutableList<ValueFilterItem>.Empty;
                if (Count != null)
                {
                    items = items.Add(new ValueFilterItem("Count", Count));
                }
                return new EdgeFilter("TestEdge", "NodeIn", "NodeOut", null, null, items);
            }
        }

        private sealed class TestEdgeFilterWithUnsupportedProperty : IEdgeFilter
        {
            public UnsupportedValueFilter? Unsupported { get; set; }

            public EdgeFilter? GetEdgeFilter()
            {
                var items = ImmutableList<ValueFilterItem>.Empty;
                if (Unsupported != null)
                {
                    items = items.Add(new ValueFilterItem("Unsupported", Unsupported));
                }
                return new EdgeFilter("TestEdge", "NodeIn", "NodeOut", null, null, items);
            }
        }

        private sealed class TestEdgeOrder : IEdgeOrder
        {
            public OrderDirection? Name { get; set; }
        }

        private sealed class TestNodeFilter : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
        }

        private sealed class MockGraphNodeFilterService : IGraphNodeFilterService
        {
            private readonly Func<bool>? _isMatchFunc;
            private readonly bool _defaultMatch;

            public MockGraphNodeFilterService(bool defaultMatch = true)
            {
                _defaultMatch = defaultMatch;
            }

            public MockGraphNodeFilterService(Func<bool> isMatchFunc)
            {
                _isMatchFunc = isMatchFunc;
            }

            public bool IsPostFilteringRequired(INodeFilter? filter)
            {
                return filter != null;
            }

            public Task<bool> IsFilterMatchAsync(INode node, INodeFilter? filter, bool consistentRead, CancellationToken cancellationToken)
            {
                return Task.FromResult(_isMatchFunc?.Invoke() ?? _defaultMatch);
            }

            public NodePushdownQueryData? TryGetNodePushdownQueryData(string type, INodeFilter? filter, INodeOrder? order, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public ImmutableList<string> GetPropertyValues(IValueFilter filter)
            {
                throw new NotImplementedException();
            }
        }

        private sealed class MockGraphQueryService : IGraphQueryService
        {
            public Task<TryGetNodesResponse> TryGetNodesAsync(TryGetNodesRequest request, CancellationToken cancellationToken)
            {
                var now = DateTime.UtcNow;
                var node = new TestNode(
                    GlobalId.Get<TestNode>(Guid.NewGuid().ToString()),
                    VersionDetail.New,
                    now,
                    now,
                    DateTime.MinValue);
                var edge = new RelayEdge<INode>("cursor", node);
                return Task.FromResult(new TryGetNodesResponse(ImmutableList.Create<RelayEdge<INode>?>(edge)));
            }

            public Task ClearAsync(CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<TryGetVersionedNodesResponse> TryGetVersionedNodesAsync(TryGetVersionedNodesRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<TryGetEdgesResponse> TryGetEdgesAsync(TryGetEdgesRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypeAsync(GetConnectionByTypeRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypeAndPropertyNameAsync(GetConnectionByTypeAndPropertyNameRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValueAsync(GetConnectionByTypePropertyNameAndValueRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<GetConnectionResponse> GetConnectionByTypePropertyNameAndValuesAsync(GetConnectionByTypePropertyNameAndValuesRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<ToEdgeQueryResponse> GetInToEdgeConnectionAsync(ToEdgeQueryRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<ToEdgeQueryResponse> GetOutToEdgeConnectionAsync(ToEdgeQueryRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task<ToEdgeQueryResponse> GetInAndOutToEdgeConnectionAsync(ToEdgeQueryRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task PutAsync(PutRequest request, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
        }

        private sealed class MockQueryablePropertyService : IGraphQueryablePropertyService
        {
            private readonly bool _isQueryable;

            public MockQueryablePropertyService(bool isQueryable = true)
            {
                _isQueryable = isQueryable;
            }

            public bool IsQueryableProperty(string typeName, string propertyName)
            {
                return _isQueryable;
            }
        }

        private sealed record TestNode(
            string Id,
            VersionDetail Version,
            DateTime CreatedAt,
            DateTime UpdatedAt,
            DateTime DeletedAt) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt);

        private sealed class UnsupportedValueFilter : IValueFilter
        {
        }

        private enum TestEnum
        {
            Value1,
            Value2
        }
    }
}
