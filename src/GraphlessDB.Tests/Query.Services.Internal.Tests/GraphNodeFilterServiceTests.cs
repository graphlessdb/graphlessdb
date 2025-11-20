/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Linq;
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

        [TestMethod]
        public async Task IsFilterMatchAsyncWithExcludedNodeReturnsFalse()
        {
            var nodeFilterDataLayerService = new ExcludeNodeGraphNodeFilterDataLayerService();
            var service = new GraphNodeFilterService(
                nodeFilterDataLayerService,
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                new EmptyGraphQueryablePropertyService(),
                new GraphSerializationService());

            var node = TestNode.New("test-value");
            var filter = new TestNodeFilter();
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithEdgeFilterNodeInReturnsTrue()
        {
            var mockQueryService = new MockGraphQueryServiceWithEdges();
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                mockQueryService,
                new GraphCursorSerializationService(),
                new EmptyGraphQueryablePropertyService(),
                new GraphSerializationService());

            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithEdgeFilter
            {
                TestEdge = new TestEdgeFilter
                {
                    NodeInFilter = new TestNodeFilter()
                }
            };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithEdgeFilterNodeInNoMatchReturnsFalse()
        {
            var mockQueryService = new MockGraphQueryServiceWithEdges();
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                mockQueryService,
                new GraphCursorSerializationService(),
                new EmptyGraphQueryablePropertyService(),
                new GraphSerializationService());

            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithEdgeFilter
            {
                TestEdge = new TestEdgeFilter
                {
                    NodeInFilter = new TestNodeFilter { CreatedAt = new DateTimeFilter { Eq = new DateTime(2099, 1, 1) } }
                }
            };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithEdgeFilterNodeOutReturnsTrue()
        {
            var mockQueryService = new MockGraphQueryServiceWithEdges();
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                mockQueryService,
                new GraphCursorSerializationService(),
                new EmptyGraphQueryablePropertyService(),
                new GraphSerializationService());

            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithEdgeFilter
            {
                TestEdge = new TestEdgeFilter
                {
                    NodeOutFilter = new TestNodeFilter()
                }
            };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithEdgeFilterNodeOutNoMatchReturnsFalse()
        {
            var mockQueryService = new MockGraphQueryServiceWithEdges();
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                mockQueryService,
                new GraphCursorSerializationService(),
                new EmptyGraphQueryablePropertyService(),
                new GraphSerializationService());

            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithEdgeFilter
            {
                TestEdge = new TestEdgeFilter
                {
                    NodeOutFilter = new TestNodeFilter { CreatedAt = new DateTimeFilter { Eq = new DateTime(2099, 1, 1) } }
                }
            };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithEdgeFilterNoEdgesReturnsFalse()
        {
            var mockQueryService = new MockGraphQueryServiceNoEdges();
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                mockQueryService,
                new GraphCursorSerializationService(),
                new EmptyGraphQueryablePropertyService(),
                new GraphSerializationService());

            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithEdgeFilter
            {
                TestEdge = new TestEdgeFilter
                {
                    NodeInFilter = new TestNodeFilter()
                }
            };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithEdgeFilterBothInAndOutReturnsTrue()
        {
            var mockQueryService = new MockGraphQueryServiceWithEdges();
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                mockQueryService,
                new GraphCursorSerializationService(),
                new EmptyGraphQueryablePropertyService(),
                new GraphSerializationService());

            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithEdgeFilter
            {
                TestEdge = new TestEdgeFilter
                {
                    NodeInFilter = new TestNodeFilter(),
                    NodeOutFilter = new TestNodeFilter()
                }
            };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncMissingPropertyThrowsException()
        {
            var service = CreateService();
            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithMissingProperty { MissingProperty = new StringFilter { Eq = "test" } };

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithStringFilterUnsupportedPropertyTypeThrowsException()
        {
            var service = CreateService();
            var node = new TestNodeWithIntProperty(
                "test-id",
                VersionDetail.New,
                DateTime.UtcNow,
                DateTime.UtcNow,
                DateTime.MinValue,
                42);
            var filter = new TestNodeFilterWithIntProperty { IntProperty = new StringFilter { Eq = "test" } };

            await Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
            {
                await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithIdFilterUnsupportedPropertyTypeThrowsException()
        {
            var service = CreateService();
            var node = new TestNodeWithIntProperty(
                "test-id",
                VersionDetail.New,
                DateTime.UtcNow,
                DateTime.UtcNow,
                DateTime.MinValue,
                42);
            var filter = new TestNodeFilterWithIntProperty { IntProperty = new IdFilter { Eq = "test" } };

            await Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
            {
                await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithDateTimeFilterUnsupportedPropertyTypeThrowsException()
        {
            var service = CreateService();
            var node = new TestNodeWithIntProperty(
                "test-id",
                VersionDetail.New,
                DateTime.UtcNow,
                DateTime.UtcNow,
                DateTime.MinValue,
                42);
            var filter = new TestNodeFilterWithIntProperty { IntProperty = new DateTimeFilter { Eq = DateTime.Now } };

            await Assert.ThrowsExceptionAsync<NotSupportedException>(async () =>
            {
                await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            });
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithUnsupportedValueFilterThrowsException()
        {
            var service = CreateService();
            var node = TestNode.New("test-value");
            var filter = new TestNodeFilterWithUnsupportedValueFilter { UnsupportedProperty = new UnsupportedFilter() };

            await Assert.ThrowsExceptionAsync<GraphlessDBOperationException>(async () =>
            {
                await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            });
        }

        [TestMethod]
        public void GetPropertyValuesWithUnsupportedGetValueFilterThrowsException()
        {
            var service = CreateService();
            var filter = new UnsupportedGetValueFilter();
            Assert.ThrowsException<NotSupportedException>(() =>
            {
                service.GetPropertyValues(filter);
            });
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithNonSupportedFilterOperatorReturnsOrderOnly()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var filter = new TestNodeFilter { CreatedAt = new DateTimeFilter { Lt = new DateTime(2025, 1, 1) } };
            var order = new TestNodeOrder { CreatedAt = OrderDirection.Asc };
            var result = service.TryGetNodePushdownQueryData("TestType", filter, order, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithIdNeFilterReturnsOrderOnly()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var filter = new TestNodeFilterWithIdProperty { Id = new IdFilter { Ne = "test-id" } };
            var order = new TestNodeOrder { CreatedAt = OrderDirection.Asc };
            var result = service.TryGetNodePushdownQueryData("TestType", filter, order, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithEnumInFilterReturnsOrderOnly()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var filter = new TestNodeFilterWithEnumProperty { EnumProperty = new EnumFilter { In = new object[] { TestEnum.Value1 } } };
            var order = new TestNodeOrder { CreatedAt = OrderDirection.Asc };
            var result = service.TryGetNodePushdownQueryData("TestType", filter, order, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithStringInFilterReturnsOrderOnly()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var filter = new TestNodeFilterWithStringProperty { StringProperty = new StringFilter { In = new[] { "test" } } };
            var order = new TestNodeOrder { CreatedAt = OrderDirection.Asc };
            var result = service.TryGetNodePushdownQueryData("TestType", filter, order, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithEnumNullValueReturnsFalse()
        {
            var service = CreateService();
            var node = new TestNodeWithNullableEnum(
                "test-id",
                VersionDetail.New,
                DateTime.UtcNow,
                DateTime.UtcNow,
                DateTime.MinValue,
                null);

            var filter = new TestNodeFilterWithNullableEnumProperty { NullableEnumProperty = new EnumFilter { Eq = TestEnum.Value1 } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithFilterHasValueFilterProperty()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var filter = new TestNodeFilterWithHasValueFilter { HasValueFilterProperty = new TestHasValueFilter { InnerFilter = new StringFilter { Eq = "test" } } };
            var result = service.TryGetNodePushdownQueryData("TestType", filter, null, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.AreEqual("HasValueFilterProperty", result.Order.PropertyName);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithIdFilterPropertyTypeMismatchThrowsException()
        {
            var service = CreateService();
            var node = TestNodeWithBoolProperty.New();

            var filter = new TestNodeFilterWithBoolProperty { Id = new IdFilter { Eq = node.Id }, BoolProp = new UnsupportedValueFilter() };
            await Assert.ThrowsExceptionAsync<NotSupportedException>(() => service.IsFilterMatchAsync(node, filter, false, CancellationToken.None));
        }

        [TestMethod]
        public void AsEntityFilterWithNullFilterReturnsEmptyFilter()
        {
            var result = GraphNodeFilterService.AsEntityFilter(null);
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.ValueFilterItems.Count);
            Assert.AreEqual(0, result.EdgeFilterItems.Count);
        }

        [TestMethod]
        public void AsEntityFilterWithValueFilterPropertyExtractsFilterItems()
        {
            var filter = new TestNodeFilter { CreatedAt = new DateTimeFilter { Eq = new DateTime(2025, 1, 1) } };
            var result = GraphNodeFilterService.AsEntityFilter(filter);
            Assert.AreEqual(1, result.ValueFilterItems.Count);
            Assert.AreEqual("CreatedAt", result.ValueFilterItems[0].Name);
        }

        [TestMethod]
        public void AsEntityFilterWithEdgeFilterPropertyExtractsEdgeFilterItems()
        {
            var filter = new TestNodeFilterWithEdgeFilter
            {
                TestEdge = new TestEdgeFilter
                {
                    NodeInFilter = new TestNodeFilter()
                }
            };
            var result = GraphNodeFilterService.AsEntityFilter(filter);
            Assert.AreEqual(1, result.EdgeFilterItems.Count);
        }

        [TestMethod]
        public void AsOrderWithNullOrderReturnsEmptyOrder()
        {
            INodeOrder? nullOrder = null;
            var result = typeof(GraphNodeFilterService)
                .GetMethod("AsOrder", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                ?.Invoke(null, new object?[] { nullOrder });

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public void AsOrderWithSingleOrderItemReturnsOrder()
        {
            var order = new TestNodeOrder { CreatedAt = OrderDirection.Asc };
            var result = typeof(GraphNodeFilterService)
                .GetMethod("AsOrder", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                ?.Invoke(null, new object?[] { order });

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNullPropertyValueForStringFilterReturnsFalse()
        {
            var service = CreateService();
            var node = new TestNodeWithNullableString(
                "test-id",
                VersionDetail.New,
                DateTime.UtcNow,
                DateTime.UtcNow,
                DateTime.MinValue,
                null);

            var filter = new TestNodeFilterWithStringProperty { StringProperty = new StringFilter { Eq = "test" } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNullPropertyValueForIdFilterReturnsFalse()
        {
            var service = CreateService();
            var node = new TestNodeWithNullableId(
                "test-id",
                VersionDetail.New,
                DateTime.UtcNow,
                DateTime.UtcNow,
                DateTime.MinValue,
                null);

            var filter = new TestNodeFilterWithNullableIdProperty { NullableId = new IdFilter { Eq = "test" } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task IsFilterMatchAsyncWithNullPropertyValueForDateTimeFilterReturnsFalse()
        {
            var service = CreateService();
            var node = new TestNodeWithNullableDateTime(
                "test-id",
                VersionDetail.New,
                DateTime.UtcNow,
                DateTime.UtcNow,
                DateTime.MinValue,
                null);

            var filter = new TestNodeFilterWithNullableDateTimeProperty { NullableDateTime = new DateTimeFilter { Eq = DateTime.Now } };
            var result = await service.IsFilterMatchAsync(node, filter, false, CancellationToken.None);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithQueryableIdFilterAndMatchingOrderReturnsDataWithFilter()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var filter = new TestNodeFilterWithIdProperty { Id = new IdFilter { Eq = "test-id" } };
            var order = new TestNodeOrderWithIdProperty { Id = OrderDirection.Asc };
            var result = service.TryGetNodePushdownQueryData("TestType", filter, order, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.AreEqual("Id", result.Order.PropertyName);
            Assert.IsNotNull(result.Filter);
            Assert.AreEqual(PropertyOperator.Equals, result.Filter.PropertyOperator);
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithQueryableEnumFilterAndMatchingOrderReturnsDataWithFilter()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var filter = new TestNodeFilterWithEnumProperty { EnumProperty = new EnumFilter { Eq = TestEnum.Value1 } };
            var order = new TestNodeOrderWithEnumProperty { EnumProperty = OrderDirection.Asc };
            var result = service.TryGetNodePushdownQueryData("TestType", filter, order, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.AreEqual("EnumProperty", result.Order.PropertyName);
            Assert.IsNotNull(result.Filter);
            Assert.AreEqual(PropertyOperator.Equals, result.Filter.PropertyOperator);
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithQueryableIdFilterNoEqReturnsOrderOnly()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var filter = new TestNodeFilterWithIdProperty { Id = new IdFilter { Ne = "test-id" } };
            var order = new TestNodeOrderWithIdProperty { Id = OrderDirection.Asc };
            var result = service.TryGetNodePushdownQueryData("TestType", filter, order, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.IsNull(result.Filter);
        }

        [TestMethod]
        public void TryGetNodePushdownQueryDataWithQueryableEnumFilterNoEqReturnsOrderOnly()
        {
            var queryablePropertyService = new TestGraphQueryablePropertyService(true);
            var service = new GraphNodeFilterService(
                new EmptyGraphNodeFilterDataLayerService(),
                new EmptyGraphQueryService(),
                new GraphCursorSerializationService(),
                queryablePropertyService,
                new GraphSerializationService());

            var filter = new TestNodeFilterWithEnumProperty { EnumProperty = new EnumFilter { In = new object[] { TestEnum.Value1 } } };
            var order = new TestNodeOrderWithEnumProperty { EnumProperty = OrderDirection.Asc };
            var result = service.TryGetNodePushdownQueryData("TestType", filter, order, CancellationToken.None);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.Order);
            Assert.IsNull(result.Filter);
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

        private sealed class TestNodeFilterWithEdgeFilter : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
            public TestEdgeFilter? TestEdge { get; set; }
        }

        private sealed class TestNodeFilterWithEdgeFilterThrowsOnNull : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
            public TestEdgeFilterThrowsOnNull? TestEdge { get; set; }
        }

        private sealed class TestNodeFilterWithEdgeFilterThrowsOnNullOut : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
            public TestEdgeFilterThrowsOnNullOut? TestEdge { get; set; }
        }

        private sealed class TestEdgeFilter : IEdgeFilter
        {
            public INodeFilter? NodeInFilter { get; set; }
            public INodeFilter? NodeOutFilter { get; set; }

            public EdgeFilter? GetEdgeFilter()
            {
                return new EdgeFilter(
                    "TestEdgeType",
                    "TestNodeInType",
                    "TestNodeOutType",
                    NodeInFilter,
                    NodeOutFilter,
                    ImmutableList<ValueFilterItem>.Empty);
            }
        }

        private sealed class TestNodeFilterWithMissingProperty : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
            public StringFilter? MissingProperty { get; set; }
        }

        private sealed class TestNodeFilterWithIntProperty : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
            public IValueFilter? IntProperty { get; set; }
        }

        private sealed class TestNodeFilterWithUnsupportedValueFilter : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
            public UnsupportedFilter? UnsupportedProperty { get; set; }
        }

        private sealed class UnsupportedGetValueFilter : IValueFilter
        {
        }

        private sealed class TestEdgeFilterThrowsOnNull : IEdgeFilter
        {
            public INodeFilter? NodeInFilter { get; set; }

            public EdgeFilter? GetEdgeFilter()
            {
                return new EdgeFilter(
                    "TestEdgeType",
                    "TestNodeInType",
                    "TestNodeOutType",
                    NodeInFilter,
                    null,
                    ImmutableList<ValueFilterItem>.Empty);
            }
        }

        private sealed class TestEdgeFilterThrowsOnNullOut : IEdgeFilter
        {
            public INodeFilter? NodeOutFilter { get; set; }

            public EdgeFilter? GetEdgeFilter()
            {
                return new EdgeFilter(
                    "TestEdgeType",
                    "TestNodeInType",
                    "TestNodeOutType",
                    null,
                    NodeOutFilter,
                    ImmutableList<ValueFilterItem>.Empty);
            }
        }

        private sealed class TestNodeFilterWithNullableEnumProperty : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
            public EnumFilter? NullableEnumProperty { get; set; }
        }

        private sealed class TestNodeFilterWithHasValueFilter : INodeFilter
        {
            public DateTimeFilter? CreatedAt { get; set; }
            public TestHasValueFilter? HasValueFilterProperty { get; set; }
        }

        private sealed class TestHasValueFilter : IHasValueFilter
        {
            public IValueFilter? InnerFilter { get; set; }

            public IValueFilter GetValueFilter()
            {
                return InnerFilter ?? throw new InvalidOperationException("InnerFilter not set");
            }
        }

        private sealed record TestNodeWithNullableEnum(
            string Id,
            VersionDetail Version,
            DateTime CreatedAt,
            DateTime UpdatedAt,
            DateTime DeletedAt,
            TestEnum? NullableEnumProperty) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt);

        private sealed record TestNodeWithIntProperty(
            string Id,
            VersionDetail Version,
            DateTime CreatedAt,
            DateTime UpdatedAt,
            DateTime DeletedAt,
            int IntProperty) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt)
        {
            public static TestNodeWithIntProperty New()
            {
                var now = DateTime.UtcNow;
                return new TestNodeWithIntProperty(
                    GlobalId.Get<TestNodeWithIntProperty>(Guid.NewGuid().ToString()),
                    VersionDetail.New,
                    now,
                    now,
                    DateTime.MinValue,
                    42);
            }
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

    internal sealed class ExcludeNodeGraphNodeFilterDataLayerService : IGraphNodeFilterDataLayerService
    {
        public Task<bool> IsNodeExcludedAsync(INode node, INodeFilter filter, CancellationToken cancellationToken)
        {
            return Task.FromResult(true);
        }

        public INodeFilter? TryGetDataLayerFilter(INodeFilter? filter, CancellationToken cancellationToken)
        {
            return filter;
        }
    }

    internal sealed class MockGraphQueryServiceWithEdges : IGraphQueryService
    {
        private sealed record MockNode(
            string Id,
            VersionDetail Version,
            DateTime CreatedAt,
            DateTime UpdatedAt,
            DateTime DeletedAt) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt);

        public Task ClearAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<TryGetNodesResponse> TryGetNodesAsync(TryGetNodesRequest request, CancellationToken cancellationToken)
        {
            var now = DateTime.UtcNow;
            var nodes = request.Ids
                .Select(id =>
                {
                    INode node = new MockNode(
                        id,
                        VersionDetail.New,
                        now,
                        now,
                        DateTime.MinValue);
                    return (RelayEdge<INode>?)new RelayEdge<INode>("cursor-" + id, node);
                })
                .ToImmutableList();

            return Task.FromResult(new TryGetNodesResponse(nodes));
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
            var edge = new RelayEdge<EdgeKey>("cursor", new EdgeKey("TestEdgeType", "in-id", "out-id"));
            var connection = new Connection<RelayEdge<EdgeKey>, EdgeKey>(
                ImmutableList.Create(edge),
                new PageInfo(false, false, "cursor", "cursor"));
            return Task.FromResult(new ToEdgeQueryResponse(connection));
        }

        public Task<ToEdgeQueryResponse> GetOutToEdgeConnectionAsync(ToEdgeQueryRequest request, CancellationToken cancellationToken)
        {
            var edge = new RelayEdge<EdgeKey>("cursor", new EdgeKey("TestEdgeType", "in-id", "out-id"));
            var connection = new Connection<RelayEdge<EdgeKey>, EdgeKey>(
                ImmutableList.Create(edge),
                new PageInfo(false, false, "cursor", "cursor"));
            return Task.FromResult(new ToEdgeQueryResponse(connection));
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

    internal sealed class MockGraphQueryServiceNoEdges : IGraphQueryService
    {
        public Task ClearAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<TryGetNodesResponse> TryGetNodesAsync(TryGetNodesRequest request, CancellationToken cancellationToken)
        {
            return Task.FromResult(new TryGetNodesResponse(ImmutableList<RelayEdge<INode>?>.Empty));
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
            var connection = new Connection<RelayEdge<EdgeKey>, EdgeKey>(
                ImmutableList<RelayEdge<EdgeKey>>.Empty,
                new PageInfo(false, false, string.Empty, string.Empty));
            return Task.FromResult(new ToEdgeQueryResponse(connection));
        }

        public Task<ToEdgeQueryResponse> GetOutToEdgeConnectionAsync(ToEdgeQueryRequest request, CancellationToken cancellationToken)
        {
            var connection = new Connection<RelayEdge<EdgeKey>, EdgeKey>(
                ImmutableList<RelayEdge<EdgeKey>>.Empty,
                new PageInfo(false, false, string.Empty, string.Empty));
            return Task.FromResult(new ToEdgeQueryResponse(connection));
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

    internal sealed class MockGraphQueryServiceEmptyConnectionThrows : IGraphQueryService
    {
        public Task ClearAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<TryGetNodesResponse> TryGetNodesAsync(TryGetNodesRequest request, CancellationToken cancellationToken)
        {
            return Task.FromResult(new TryGetNodesResponse(ImmutableList<RelayEdge<INode>?>.Empty));
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
            var edge = new RelayEdge<EdgeKey>("cursor", new EdgeKey("TestEdgeType", "in-id", "out-id"));
            var connection = new Connection<RelayEdge<EdgeKey>, EdgeKey>(
                ImmutableList.Create(edge),
                new PageInfo(false, false, "cursor", "cursor"));
            return Task.FromResult(new ToEdgeQueryResponse(connection));
        }

        public Task<ToEdgeQueryResponse> GetOutToEdgeConnectionAsync(ToEdgeQueryRequest request, CancellationToken cancellationToken)
        {
            var edge = new RelayEdge<EdgeKey>("cursor", new EdgeKey("TestEdgeType", "in-id", "out-id"));
            var connection = new Connection<RelayEdge<EdgeKey>, EdgeKey>(
                ImmutableList.Create(edge),
                new PageInfo(false, false, "cursor", "cursor"));
            return Task.FromResult(new ToEdgeQueryResponse(connection));
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

    internal sealed class UnsupportedValueFilter : IValueFilter
    {
    }

    internal sealed record TestNodeWithBoolProperty(string Id, VersionDetail Version, DateTime CreatedAt, DateTime UpdatedAt, DateTime DeletedAt) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt)
    {
        public bool BoolProp { get; set; }

        public static TestNodeWithBoolProperty New()
        {
            var now = DateTime.UtcNow;
            return new TestNodeWithBoolProperty(
                GlobalId.Get<TestNodeWithBoolProperty>(Guid.NewGuid().ToString()),
                VersionDetail.New,
                now,
                now,
                DateTime.MinValue)
            {
                BoolProp = true
            };
        }
    }

    internal sealed class TestNodeFilterWithBoolProperty : INodeFilter
    {
        public IdFilter? Id { get; set; }
        public DateTimeFilter? CreatedAt { get; set; }
        public UnsupportedValueFilter? BoolProp { get; set; }
    }

    internal sealed class TestNodeFilterWithDateTimeFilterNoEq : INodeFilter
    {
        public DateTimeFilter? CreatedAt { get; set; }
    }

    internal sealed class TestNodeFilterWithEnumFilterNoEq : INodeFilter
    {
        public DateTimeFilter? CreatedAt { get; set; }
        public EnumFilter? Status { get; set; }
    }

    internal sealed class TestNodeFilterWithIdFilterNoEq : INodeFilter
    {
        public DateTimeFilter? CreatedAt { get; set; }
        public IdFilter? Id { get; set; }
        public IdFilter? OtherId { get; set; }
    }

    internal sealed class TestNodeFilterWithStringFilterNoEqNoBeginsWith : INodeFilter
    {
        public DateTimeFilter? CreatedAt { get; set; }
        public StringFilter? Name { get; set; }
    }

    internal sealed class NonQueryablePropertyService : IGraphQueryablePropertyService
    {
        public bool IsQueryableProperty(string typeName, string propertyName)
        {
            return false;
        }
    }

    internal sealed record TestNodeWithNullableString(
        string Id,
        VersionDetail Version,
        DateTime CreatedAt,
        DateTime UpdatedAt,
        DateTime DeletedAt,
        string? StringProperty) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt);

    internal sealed record TestNodeWithNullableId(
        string Id,
        VersionDetail Version,
        DateTime CreatedAt,
        DateTime UpdatedAt,
        DateTime DeletedAt,
        string? NullableId) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt);

    internal sealed record TestNodeWithNullableDateTime(
        string Id,
        VersionDetail Version,
        DateTime CreatedAt,
        DateTime UpdatedAt,
        DateTime DeletedAt,
        DateTime? NullableDateTime) : INode(Id, Version, CreatedAt, UpdatedAt, DeletedAt);

    internal sealed class TestNodeFilterWithNullableIdProperty : INodeFilter
    {
        public DateTimeFilter? CreatedAt { get; set; }
        public IdFilter? NullableId { get; set; }
    }

    internal sealed class TestNodeFilterWithNullableDateTimeProperty : INodeFilter
    {
        public DateTimeFilter? CreatedAt { get; set; }
        public DateTimeFilter? NullableDateTime { get; set; }
    }

    internal sealed class TestNodeOrderWithIdProperty : INodeOrder
    {
        public OrderDirection? Id { get; set; }
    }

    internal sealed class TestNodeOrderWithEnumProperty : INodeOrder
    {
        public OrderDirection? EnumProperty { get; set; }
    }
}
