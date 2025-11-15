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
    public sealed class GraphNodeFilterServiceTests
    {
        private static GraphNodeFilterService CreateService()
        {
            var nodeFilterDataLayerService = new EmptyGraphNodeFilterDataLayerService();
            var graphDataQueryService = new EmptyGraphQueryService();
            var cursorSerializer = new GraphCursorSerializationService();
            var queryablePropertyService = new EmptyGraphQueryablePropertyService();
            var entityValueSerializer = new GraphSerializationService();

            return new GraphNodeFilterService(
                nodeFilterDataLayerService,
                graphDataQueryService,
                cursorSerializer,
                queryablePropertyService,
                entityValueSerializer);
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
            var filter = new TestNodeFilter();
            var result = service.IsPostFilteringRequired(filter);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void GetPropertyValuesWithDateTimeFilterEqReturnsSerializedValue()
        {
            var service = CreateService();
            var filter = new DateTimeFilter { Eq = new DateTime(2025, 1, 1) };
            var result = service.GetPropertyValues(filter);
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("3耐638712864000000000", result[0]);
        }

        [TestMethod]
        public void GetPropertyValuesWithDateTimeFilterNoEqReturnsEmpty()
        {
            var service = CreateService();
            var filter = new DateTimeFilter { Ge = new DateTime(2025, 1, 1) };
            var result = service.GetPropertyValues(filter);
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void GetPropertyValuesWithEnumFilterEqReturnsSerializedValue()
        {
            var service = CreateService();
            var filter = new EnumFilter { Eq = TestEnum.Value1 };
            var result = service.GetPropertyValues(filter);
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("Value1", result[0]);
        }

        [TestMethod]
        public void GetPropertyValuesWithEnumFilterNoEqReturnsEmpty()
        {
            var service = CreateService();
            var filter = new EnumFilter { In = new object[] { TestEnum.Value1 } };
            var result = service.GetPropertyValues(filter);
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void GetPropertyValuesWithIdFilterEqReturnsSerializedValue()
        {
            var service = CreateService();
            var filter = new IdFilter { Eq = "test-id" };
            var result = service.GetPropertyValues(filter);
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("test-id", result[0]);
        }

        [TestMethod]
        public void GetPropertyValuesWithIdFilterNoEqReturnsEmpty()
        {
            var service = CreateService();
            var filter = new IdFilter { Ne = "test-id" };
            var result = service.GetPropertyValues(filter);
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void GetPropertyValuesWithStringFilterEqReturnsSerializedValue()
        {
            var service = CreateService();
            var filter = new StringFilter { Eq = "test-value" };
            var result = service.GetPropertyValues(filter);
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("test-value", result[0]);
        }

        [TestMethod]
        public void GetPropertyValuesWithStringFilterBeginsWithReturnsSerializedValue()
        {
            var service = CreateService();
            var filter = new StringFilter { BeginsWith = "test-prefix" };
            var result = service.GetPropertyValues(filter);
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("test-prefix", result[0]);
        }

        [TestMethod]
        public void GetPropertyValuesWithStringFilterBeginsWithAnyReturnsMultipleSerializedValues()
        {
            var service = CreateService();
            var filter = new StringFilter { BeginsWithAny = new[] { "prefix1", "prefix2", "prefix3" } };
            var result = service.GetPropertyValues(filter);
            Assert.AreEqual(3, result.Count);
            Assert.AreEqual("prefix1", result[0]);
            Assert.AreEqual("prefix2", result[1]);
            Assert.AreEqual("prefix3", result[2]);
        }

        [TestMethod]
        public void GetPropertyValuesWithStringFilterNoMatchingPropertyReturnsEmpty()
        {
            var service = CreateService();
            var filter = new StringFilter { Ne = "test-value" };
            var result = service.GetPropertyValues(filter);
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithNullFilterAndNullOrderReturnsNull()
        {
            var service = CreateService();
            var result = service.TryGetNodePushdownQueryData("TestType", null, null, CancellationToken.None);
            Assert.IsNull(result);
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithNonQueryablePropertyThrowsException()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(false);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var order = new TestNodeOrder { CreatedAt = OrderDirection.Asc };

            Assert.ThrowsException<GraphlessDBOperationException>(() =>
            {
                service.TryGetNodePushdownQueryData("TestType", null, order, CancellationToken.None);
            });
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithQueryableOrderReturnsData()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var order = new TestNodeOrder { CreatedAt = OrderDirection.Asc };
            var result = service.TryGetNodePushdownQueryData("TestType", null, order, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.AreEqual("CreatedAt", result.Order.PropertyName);
            Assert.AreEqual(OrderDirection.Asc, result.Order.Direction);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithFilterAndOrderReturnsDataWithFilter()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var filter = new TestNodeFilter { CreatedAt = new DateTimeFilter { Eq = new DateTime(2025, 1, 1) } };
            var order = new TestNodeOrder { CreatedAt = OrderDirection.Asc };
            var result = service.TryGetNodePushdownQueryData("TestType", filter, order, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.AreEqual("CreatedAt", result.Order.PropertyName);
            Assert.IsNotNull(result.Filter);
            Assert.AreEqual(PropertyOperator.Equals, result.Filter.PropertyOperator);
            Assert.AreEqual(1, result.Filter.PropertyValues.Count);
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithQueryableFilterButNoOrderReturnsData()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var filter = new TestNodeFilter { CreatedAt = new DateTimeFilter { Eq = new DateTime(2025, 1, 1) } };
            var result = service.TryGetNodePushdownQueryData("TestType", filter, null, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.AreEqual("CreatedAt", result.Order.PropertyName);
            Assert.AreEqual(OrderDirection.Asc, result.Order.Direction);
            Assert.IsNotNull(result.Filter);
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithNonPushdownFilterReturnsOrderOnly()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var filter = new TestNodeFilter { CreatedAt = new DateTimeFilter { Ge = new DateTime(2025, 1, 1) } };
            var order = new TestNodeOrder { CreatedAt = OrderDirection.Asc };
            var result = service.TryGetNodePushdownQueryData("TestType", filter, order, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.AreEqual("CreatedAt", result.Order.PropertyName);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNullFilterReturnsTrue()
        {
            var service = CreateService();
            var node = TestNode.New("test-node");
            var result = await service.IsFilterMatchAsync(node, null, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithMatchingDateTimeFilterReturnsTrue()
        {
            var service = CreateService();
            var testDate = new DateTime(2025, 1, 1);
            var node = new TestNode(
                "test-id",
                VersionDetail.New,
                testDate,
                testDate,
                DateTime.MinValue,
                "test-value");

            var filter = new TestNodeFilter { CreatedAt = new DateTimeFilter { Eq = testDate } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNonMatchingDateTimeFilterReturnsFalse()
        {
            var service = CreateService();
            var node = TestNode.New("test-node");
            var filter = new TestNodeFilter { CreatedAt = new DateTimeFilter { Eq = new DateTime(2025, 1, 1) } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithMatchingStringPropertyReturnsTrue()
        {
            var service = CreateService();
            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithStringProperty { StringProperty = new StringFilter { Eq = "test-value" } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNonMatchingStringPropertyReturnsFalse()
        {
            var service = CreateService();
            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithStringProperty { StringProperty = new StringFilter { Eq = "other-value" } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithMatchingIdFilterReturnsTrue()
        {
            var service = CreateService();
            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithIdProperty { Id = new IdFilter { Eq = node.Id } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNonMatchingIdFilterReturnsFalse()
        {
            var service = CreateService();
            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithIdProperty { Id = new IdFilter { Eq = "different-id" } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithMatchingEnumFilterReturnsTrue()
        {
            var service = CreateService();
            var node = new TestNodeWithEnum(
                "test-id",
                VersionDetail.New,
                DateTime.UtcNow,
                DateTime.UtcNow,
                DateTime.MinValue,
                TestEnum.Value1);

            var filter = new TestNodeFilterWithEnumProperty { EnumProperty = new EnumFilter { Eq = TestEnum.Value1 } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNonMatchingEnumFilterReturnsFalse()
        {
            var service = CreateService();
            var node = new TestNodeWithEnum(
                "test-id",
                VersionDetail.New,
                DateTime.UtcNow,
                DateTime.UtcNow,
                DateTime.MinValue,
                TestEnum.Value1);

            var filter = new TestNodeFilterWithEnumProperty { EnumProperty = new EnumFilter { Eq = TestEnum.Value2 } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithStringBeginsWithMatchReturnsTrue()
        {
            var service = CreateService();
            var node = TestNode.New("test-prefix-value");
            var filter = new TestNodeFilterWithStringProperty { StringProperty = new StringFilter { BeginsWith = "test-prefix" } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithStringBeginsWithNoMatchReturnsFalse()
        {
            var service = CreateService();
            var node = TestNode.New("other-value");
            var filter = new TestNodeFilterWithStringProperty { StringProperty = new StringFilter { BeginsWith = "test-prefix" } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithStringContainsMatchReturnsTrue()
        {
            var service = CreateService();
            var node = TestNode.New("this-contains-test-here");
            var filter = new TestNodeFilterWithStringProperty { StringProperty = new StringFilter { Contains = "test" } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithStringNotContainsMatchReturnsTrue()
        {
            var service = CreateService();
            var node = TestNode.New("safe-value");
            var filter = new TestNodeFilterWithStringProperty { StringProperty = new StringFilter { NotContains = "forbidden" } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithStringInMatchReturnsTrue()
        {
            var service = CreateService();
            var node = TestNode.New("option2");
            var filter = new TestNodeFilterWithStringProperty { StringProperty = new StringFilter { In = new[] { "option1", "option2", "option3" } } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithStringNeMatchReturnsTrue()
        {
            var service = CreateService();
            var node = TestNode.New("value1");
            var filter = new TestNodeFilterWithStringProperty { StringProperty = new StringFilter { Ne = "value2" } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithIdInMatchReturnsTrue()
        {
            var service = CreateService();
            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithIdProperty { Id = new IdFilter { In = new[] { "id1", node.Id, "id3" } } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithIdNeMatchReturnsTrue()
        {
            var service = CreateService();
            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithIdProperty { Id = new IdFilter { Ne = "different-id" } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithDateTimeGeMatchReturnsTrue()
        {
            var service = CreateService();
            var testDate = new DateTime(2025, 1, 15);
            var node = new TestNode(
                "test-id",
                VersionDetail.New,
                testDate,
                testDate,
                DateTime.MinValue,
                "test-value");

            var filter = new TestNodeFilter { CreatedAt = new DateTimeFilter { Ge = new DateTime(2025, 1, 1) } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithDateTimeGtMatchReturnsTrue()
        {
            var service = CreateService();
            var testDate = new DateTime(2025, 1, 15);
            var node = new TestNode(
                "test-id",
                VersionDetail.New,
                testDate,
                testDate,
                DateTime.MinValue,
                "test-value");

            var filter = new TestNodeFilter { CreatedAt = new DateTimeFilter { Gt = new DateTime(2025, 1, 1) } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithDateTimeLeMatchReturnsTrue()
        {
            var service = CreateService();
            var testDate = new DateTime(2025, 1, 1);
            var node = new TestNode(
                "test-id",
                VersionDetail.New,
                testDate,
                testDate,
                DateTime.MinValue,
                "test-value");

            var filter = new TestNodeFilter { CreatedAt = new DateTimeFilter { Le = new DateTime(2025, 1, 15) } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithDateTimeLtMatchReturnsTrue()
        {
            var service = CreateService();
            var testDate = new DateTime(2025, 1, 1);
            var node = new TestNode(
                "test-id",
                VersionDetail.New,
                testDate,
                testDate,
                DateTime.MinValue,
                "test-value");

            var filter = new TestNodeFilter { CreatedAt = new DateTimeFilter { Lt = new DateTime(2025, 1, 15) } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithMultipleFiltersAllMatchReturnsTrue()
        {
            var service = CreateService();
            var testDate = new DateTime(2025, 1, 1);
            var node = new TestNode(
                "test-id",
                VersionDetail.New,
                testDate,
                testDate,
                DateTime.MinValue,
                "test-value");

            var filter = new TestNodeFilterWithStringProperty
            {
                CreatedAt = new DateTimeFilter { Eq = testDate },
                StringProperty = new StringFilter { BeginsWith = "test" }
            };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithMultipleFiltersOneFailsReturnsFalse()
        {
            var service = CreateService();
            var testDate = new DateTime(2025, 1, 1);
            var node = new TestNode(
                "test-id",
                VersionDetail.New,
                testDate,
                testDate,
                DateTime.MinValue,
                "test-value");

            var filter = new TestNodeFilterWithStringProperty
            {
                CreatedAt = new DateTimeFilter { Eq = testDate },
                StringProperty = new StringFilter { BeginsWith = "other" }
            };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void GetPropertyValuesWithUnsupportedFilterThrowsException()
        {
            var service = CreateService();
            var filter = new UnsupportedFilter();
            Assert.ThrowsException<NotSupportedException>(() =>
            {
                service.GetPropertyValues(filter);
            });
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithMultipleOrderItemsThrowsException()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var order = new TestMultipleNodeOrder
            {
                CreatedAt = OrderDirection.Asc,
                UpdatedAt = OrderDirection.Desc
            };

            Assert.ThrowsException<GraphlessDBOperationException>(() =>
            {
                service.TryGetNodePushdownQueryData("TestType", null, order, CancellationToken.None);
            });
        }

        private sealed class TestNodeFilter : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
        }

        private sealed class TestNodeFilterWithStringProperty : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
            public StringFilter? StringProperty { get; set; }
        }

        private sealed class TestNodeFilterWithIdProperty : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
            public IdFilter? Id { get; set; }
        }

        private sealed class TestNodeFilterWithEnumProperty : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
            public EnumFilter? EnumProperty { get; set; }
        }

        private sealed record TestNode(
            string Id,
            VersionDetail Version,
            DateTime CreatedAt,
            DateTime UpdatedAt,
            DateTime DeletedAt,
            string StringProperty) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt)
        {
            public static TestNode New(string stringProperty)
            {
                var now = DateTime.UtcNow;
                return new TestNode(
                    GlobalId.Get<TestNode>(Guid.NewGuid().ToString()),
                    VersionDetail.New,
                    now,
                    now,
                    DateTime.MinValue,
                    stringProperty);
            }
        }

        private sealed record TestNodeWithEnum(
            string Id,
            VersionDetail Version,
            DateTime CreatedAt,
            DateTime UpdatedAt,
            DateTime DeletedAt,
            TestEnum EnumProperty) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt);

        private sealed class TestNodeOrder : INodeOrder
        {
            public OrderDirection? CreatedAt { get; set; }
        }

        private sealed class TestMultipleNodeOrder : INodeOrder
        {
            public OrderDirection? CreatedAt { get; set; }
            public OrderDirection? UpdatedAt { get; set; }
        }

        private sealed class UnsupportedFilter : IValueFilter
        {
        }

        private enum TestEnum
        {
            Value1 = 0,
            Value2 = 1
        }
    }

    internal sealed class TestGraphQueryablePropertyService : IGraphQueryablePropertyService
    {
        private readonly bool _isQueryable;

        public TestGraphQueryablePropertyService(bool isQueryable)
        {
            _isQueryable = isQueryable;
        }

        public bool IsQueryableProperty(string typeName, string propertyName)
        {
            return _isQueryable;
        }
    }

    internal sealed class EmptyGraphNodeFilterDataLayerService : IGraphNodeFilterDataLayerService
    {
        public Task<bool> IsNodeExcludedAsync(INode node, INodeFilter filter, CancellationToken cancellationToken)
        {
            return Task.FromResult(false);
        }

        public INodeFilter? TryGetDataLayerFilter(INodeFilter? filter, CancellationToken cancellationToken)
        {
            return filter;
        }
    }

    internal sealed class EmptyGraphQueryService : IGraphQueryService
    {
        public Task ClearAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<TryGetNodesResponse> TryGetNodesAsync(TryGetNodesRequest request, CancellationToken cancellationToken)
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
}
