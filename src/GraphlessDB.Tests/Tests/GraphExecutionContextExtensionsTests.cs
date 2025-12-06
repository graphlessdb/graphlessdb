/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using GraphlessDB.Collections;
using GraphlessDB.Query;
using GraphlessDB.Query.Services;
using GraphlessDB.Query.Services.Internal.Tests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphlessDB.Tests
{
    [TestClass]
    public sealed class GraphExecutionContextExtensionsTests
    {
        private sealed record TestGraphResult(
            string? ChildCursor,
            string Cursor,
            bool NeedsMoreData,
            bool HasMoreData) : GraphResult(ChildCursor, Cursor, NeedsMoreData, HasMoreData);

        private static GraphExecutionContext CreateContextWithResults(
            ImmutableTree<string, GraphQueryNode> query,
            params (string key, GraphResult result)[] results)
        {
            var resultItems = results.ToImmutableDictionary(r => r.key, r => r.result);
            return new GraphExecutionContext(
                new EmptyGraphQueryExecutionService(),
                query,
                resultItems);
        }

        private static ImmutableTree<string, GraphQueryNode> CreateSimpleQuery(string rootKey = "root")
        {
            var query = new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, true, null);
            return ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(rootKey, new GraphQueryNode(query));
        }

        private static ImmutableTree<string, GraphQueryNode> CreateQueryWithChildren(
            string rootKey,
            params string[] childKeys)
        {
            var query = new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, true, null);
            var tree = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(rootKey, new GraphQueryNode(query));

            foreach (var childKey in childKeys)
            {
                tree = tree.AddNode(childKey, new GraphQueryNode(query));
                tree = tree.AddEdge(childKey, rootKey);
            }

            return tree;
        }

        #region GetRootResult Tests

        [TestMethod]
        public void GetRootResultReturnsRootResult()
        {
            var rootKey = "root";
            var query = CreateSimpleQuery(rootKey);
            var result = new TestGraphResult(null, "cursor1", false, false);
            var context = CreateContextWithResults(query, (rootKey, result));

            var rootResult = context.GetRootResult<TestGraphResult>();

            Assert.IsNotNull(rootResult);
            Assert.AreEqual("cursor1", rootResult.Cursor);
        }

        [TestMethod]
        [ExpectedException(typeof(GraphlessDBOperationException))]
        public void GetRootResultThrowsWhenResultNotFound()
        {
            var rootKey = "root";
            var query = CreateSimpleQuery(rootKey);
            var context = CreateContextWithResults(query);

            context.GetRootResult<TestGraphResult>();
        }

        #endregion

        #region TryGetRootResult Tests

        [TestMethod]
        public void TryGetRootResultReturnsRootResult()
        {
            var rootKey = "root";
            var query = CreateSimpleQuery(rootKey);
            var result = new TestGraphResult(null, "cursor1", false, false);
            var context = CreateContextWithResults(query, (rootKey, result));

            var rootResult = context.TryGetRootResult<TestGraphResult>();

            Assert.IsNotNull(rootResult);
            Assert.AreEqual("cursor1", rootResult.Cursor);
        }

        [TestMethod]
        public void TryGetRootResultReturnsNullWhenResultNotFound()
        {
            var rootKey = "root";
            var query = CreateSimpleQuery(rootKey);
            var context = CreateContextWithResults(query);

            var rootResult = context.TryGetRootResult<TestGraphResult>();

            Assert.IsNull(rootResult);
        }

        #endregion

        #region TryGetParentResult Tests

        [TestMethod]
        public void TryGetParentResultReturnsParentResult()
        {
            var rootKey = "root";
            var childKey = "child1";
            var query = CreateQueryWithChildren(rootKey, childKey);
            var parentResult = new TestGraphResult(null, "parentCursor", false, false);
            var context = CreateContextWithResults(query, (rootKey, parentResult));

            var result = context.TryGetParentResult<TestGraphResult>(childKey);

            Assert.IsNotNull(result);
            Assert.AreEqual("parentCursor", result.Cursor);
        }

        [TestMethod]
        public void TryGetParentResultReturnsNullWhenNoParent()
        {
            var rootKey = "root";
            var query = CreateSimpleQuery(rootKey);
            var context = CreateContextWithResults(query);

            var result = context.TryGetParentResult<TestGraphResult>(rootKey);

            Assert.IsNull(result);
        }

        [TestMethod]
        public void TryGetParentResultReturnsNullWhenParentResultNotFound()
        {
            var rootKey = "root";
            var childKey = "child1";
            var query = CreateQueryWithChildren(rootKey, childKey);
            // Note: Parent key exists but result is not in ResultItems
            var context = CreateContextWithResults(query);

            // This will throw because GetResult is called on line 37 when parentKey exists
            // but the result is not in ResultItems
            Assert.ThrowsException<KeyNotFoundException>(() => 
                context.TryGetParentResult<TestGraphResult>(childKey));
        }

        #endregion

        #region TryGetResult Tests

        [TestMethod]
        public void TryGetResultReturnsResult()
        {
            var key = "test";
            var query = CreateSimpleQuery(key);
            var result = new TestGraphResult(null, "cursor1", false, false);
            var context = CreateContextWithResults(query, (key, result));

            var foundResult = context.TryGetResult<TestGraphResult>(key);

            Assert.IsNotNull(foundResult);
            Assert.AreEqual("cursor1", foundResult.Cursor);
        }

        [TestMethod]
        public void TryGetResultReturnsNullWhenNotFound()
        {
            var query = CreateSimpleQuery("root");
            var context = CreateContextWithResults(query);

            var result = context.TryGetResult<TestGraphResult>("nonexistent");

            Assert.IsNull(result);
        }

        #endregion

        #region GetParentResult Tests

        [TestMethod]
        public void GetParentResultReturnsParentResult()
        {
            var rootKey = "root";
            var childKey = "child1";
            var query = CreateQueryWithChildren(rootKey, childKey);
            var parentResult = new TestGraphResult(null, "parentCursor", false, false);
            var context = CreateContextWithResults(query, (rootKey, parentResult));

            var result = context.GetParentResult<TestGraphResult>(childKey);

            Assert.IsNotNull(result);
            Assert.AreEqual("parentCursor", result.Cursor);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetParentResultThrowsWhenNoParent()
        {
            var rootKey = "root";
            var query = CreateSimpleQuery(rootKey);
            var context = CreateContextWithResults(query);

            context.GetParentResult<TestGraphResult>(rootKey);
        }

        #endregion

        #region GetChildResults Tests

        [TestMethod]
        public void GetChildResultsReturnsAllChildResults()
        {
            var rootKey = "root";
            var childKey1 = "child1";
            var childKey2 = "child2";
            var query = CreateQueryWithChildren(rootKey, childKey1, childKey2);
            var child1Result = new TestGraphResult(null, "cursor1", false, false);
            var child2Result = new TestGraphResult(null, "cursor2", false, false);
            var context = CreateContextWithResults(
                query,
                (childKey1, child1Result),
                (childKey2, child2Result));

            var results = context.GetChildResults(rootKey);

            Assert.AreEqual(2, results.Count);
            Assert.IsTrue(results.Any(r => r.Cursor == "cursor1"));
            Assert.IsTrue(results.Any(r => r.Cursor == "cursor2"));
        }

        [TestMethod]
        public void GetChildResultsReturnsEmptyListWhenNoChildren()
        {
            var rootKey = "root";
            var query = CreateSimpleQuery(rootKey);
            var context = CreateContextWithResults(query);

            var results = context.GetChildResults(rootKey);

            Assert.AreEqual(0, results.Count);
        }

        #endregion

        #region TryGetChildResults Tests

        [TestMethod]
        public void TryGetChildResultsReturnsAllChildResults()
        {
            var rootKey = "root";
            var childKey1 = "child1";
            var childKey2 = "child2";
            var query = CreateQueryWithChildren(rootKey, childKey1, childKey2);
            var child1Result = new TestGraphResult(null, "cursor1", false, false);
            var child2Result = new TestGraphResult(null, "cursor2", false, false);
            var context = CreateContextWithResults(
                query,
                (childKey1, child1Result),
                (childKey2, child2Result));

            var results = context.TryGetChildResults(rootKey);

            Assert.AreEqual(2, results.Count);
            Assert.IsTrue(results.Any(r => r?.Cursor == "cursor1"));
            Assert.IsTrue(results.Any(r => r?.Cursor == "cursor2"));
        }

        [TestMethod]
        public void TryGetChildResultsReturnsEmptyListWhenNoChildren()
        {
            var rootKey = "root";
            var query = CreateSimpleQuery(rootKey);
            var context = CreateContextWithResults(query);

            var results = context.TryGetChildResults(rootKey);

            Assert.AreEqual(0, results.Count);
        }

        [TestMethod]
        public void TryGetChildResultsReturnsNullsForMissingResults()
        {
            var rootKey = "root";
            var childKey1 = "child1";
            var childKey2 = "child2";
            var query = CreateQueryWithChildren(rootKey, childKey1, childKey2);
            var child1Result = new TestGraphResult(null, "cursor1", false, false);
            var context = CreateContextWithResults(query, (childKey1, child1Result));

            var results = context.TryGetChildResults(rootKey);

            Assert.AreEqual(2, results.Count);
            Assert.IsTrue(results.Any(r => r?.Cursor == "cursor1"));
            Assert.IsTrue(results.Any(r => r == null));
        }

        #endregion

        #region GetSingleChildResult Tests

        [TestMethod]
        public void GetSingleChildResultReturnsSingleChild()
        {
            var rootKey = "root";
            var childKey = "child1";
            var query = CreateQueryWithChildren(rootKey, childKey);
            var childResult = new TestGraphResult(null, "cursor1", false, false);
            var context = CreateContextWithResults(query, (childKey, childResult));

            var result = context.GetSingleChildResult<TestGraphResult>(rootKey);

            Assert.IsNotNull(result);
            Assert.AreEqual("cursor1", result.Cursor);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetSingleChildResultThrowsWhenNoChildren()
        {
            var rootKey = "root";
            var query = CreateSimpleQuery(rootKey);
            var context = CreateContextWithResults(query);

            context.GetSingleChildResult<TestGraphResult>(rootKey);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetSingleChildResultThrowsWhenMultipleChildren()
        {
            var rootKey = "root";
            var childKey1 = "child1";
            var childKey2 = "child2";
            var query = CreateQueryWithChildren(rootKey, childKey1, childKey2);
            var child1Result = new TestGraphResult(null, "cursor1", false, false);
            var child2Result = new TestGraphResult(null, "cursor2", false, false);
            var context = CreateContextWithResults(
                query,
                (childKey1, child1Result),
                (childKey2, child2Result));

            context.GetSingleChildResult<TestGraphResult>(rootKey);
        }

        #endregion

        #region TryGetSingleChildResult Tests

        [TestMethod]
        public void TryGetSingleChildResultReturnsSingleChild()
        {
            var rootKey = "root";
            var childKey = "child1";
            var query = CreateQueryWithChildren(rootKey, childKey);
            var childResult = new TestGraphResult(null, "cursor1", false, false);
            var context = CreateContextWithResults(query, (childKey, childResult));

            var result = context.TryGetSingleChildResult<TestGraphResult>(rootKey);

            Assert.IsNotNull(result);
            Assert.AreEqual("cursor1", result.Cursor);
        }

        [TestMethod]
        public void TryGetSingleChildResultReturnsNullWhenNoChildren()
        {
            var rootKey = "root";
            var query = CreateSimpleQuery(rootKey);
            var context = CreateContextWithResults(query);

            var result = context.TryGetSingleChildResult<TestGraphResult>(rootKey);

            Assert.IsNull(result);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TryGetSingleChildResultThrowsWhenMultipleChildren()
        {
            var rootKey = "root";
            var childKey1 = "child1";
            var childKey2 = "child2";
            var query = CreateQueryWithChildren(rootKey, childKey1, childKey2);
            var child1Result = new TestGraphResult(null, "cursor1", false, false);
            var child2Result = new TestGraphResult(null, "cursor2", false, false);
            var context = CreateContextWithResults(
                query,
                (childKey1, child1Result),
                (childKey2, child2Result));

            context.TryGetSingleChildResult<TestGraphResult>(rootKey);
        }

        #endregion

        #region GetResult Tests

        [TestMethod]
        public void GetResultReturnsResult()
        {
            var key = "test";
            var query = CreateSimpleQuery(key);
            var result = new TestGraphResult(null, "cursor1", false, false);
            var context = CreateContextWithResults(query, (key, result));

            var foundResult = context.GetResult<TestGraphResult>(key);

            Assert.IsNotNull(foundResult);
            Assert.AreEqual("cursor1", foundResult.Cursor);
        }

        [TestMethod]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void GetResultThrowsWhenNotFound()
        {
            var query = CreateSimpleQuery("root");
            var context = CreateContextWithResults(query);

            context.GetResult<TestGraphResult>("nonexistent");
        }

        #endregion

        #region SetResult Tests

        [TestMethod]
        public void SetResultAddsNewResult()
        {
            var key = "test";
            var query = CreateSimpleQuery(key);
            var context = CreateContextWithResults(query);
            var result = new TestGraphResult(null, "cursor1", false, false);

            var newContext = context.SetResult(key, result);

            Assert.IsNotNull(newContext);
            var foundResult = newContext.GetResult<TestGraphResult>(key);
            Assert.AreEqual("cursor1", foundResult.Cursor);
        }

        [TestMethod]
        public void SetResultUpdatesExistingResult()
        {
            var key = "test";
            var query = CreateSimpleQuery(key);
            var oldResult = new TestGraphResult(null, "cursor1", false, false);
            var context = CreateContextWithResults(query, (key, oldResult));
            var newResult = new TestGraphResult(null, "cursor2", true, true);

            var newContext = context.SetResult(key, newResult);

            var foundResult = newContext.GetResult<TestGraphResult>(key);
            Assert.AreEqual("cursor2", foundResult.Cursor);
            Assert.AreEqual(true, foundResult.NeedsMoreData);
            Assert.AreEqual(true, foundResult.HasMoreData);
        }

        [TestMethod]
        public void SetResultDoesNotModifyOriginalContext()
        {
            var key = "test";
            var query = CreateSimpleQuery(key);
            var context = CreateContextWithResults(query);
            var result = new TestGraphResult(null, "cursor1", false, false);

            var newContext = context.SetResult(key, result);

            Assert.AreEqual(0, context.ResultItems.Count);
            Assert.AreEqual(1, newContext.ResultItems.Count);
        }

        #endregion

        #region GetQuery Tests

        [TestMethod]
        public void GetQueryReturnsQuery()
        {
            var key = "test";
            var query = CreateSimpleQuery(key);
            var context = CreateContextWithResults(query);

            var foundQuery = context.GetQuery<NodeConnectionQuery>(key);

            Assert.IsNotNull(foundQuery);
            Assert.AreEqual("User", foundQuery.Type);
        }

        [TestMethod]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void GetQueryThrowsWhenKeyNotFound()
        {
            var query = CreateSimpleQuery("root");
            var context = CreateContextWithResults(query);

            context.GetQuery<NodeConnectionQuery>("nonexistent");
        }

        #endregion

        #region GetSingleChildQuery Tests

        [TestMethod]
        public void GetSingleChildQueryReturnsChildQuery()
        {
            var rootKey = "root";
            var childKey = "child1";
            var query = CreateQueryWithChildren(rootKey, childKey);
            var context = CreateContextWithResults(query);

            var childQuery = context.GetSingleChildQuery(rootKey);

            Assert.IsNotNull(childQuery);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetSingleChildQueryThrowsWhenNoChildren()
        {
            var rootKey = "root";
            var query = CreateSimpleQuery(rootKey);
            var context = CreateContextWithResults(query);

            context.GetSingleChildQuery(rootKey);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetSingleChildQueryThrowsWhenMultipleChildren()
        {
            var rootKey = "root";
            var childKey1 = "child1";
            var childKey2 = "child2";
            var query = CreateQueryWithChildren(rootKey, childKey1, childKey2);
            var context = CreateContextWithResults(query);

            context.GetSingleChildQuery(rootKey);
        }

        #endregion

        #region TryFindResult Tests

        [TestMethod]
        public void TryFindResultFindsRootKey()
        {
            var rootKey = "root";
            var query = CreateSimpleQuery(rootKey);
            var context = CreateContextWithResults(query);

            var found = context.TryFindResult(k => k == rootKey, out var resultKey);

            Assert.IsTrue(found);
            Assert.AreEqual(rootKey, resultKey);
        }

        [TestMethod]
        public void TryFindResultFindsChildKey()
        {
            var rootKey = "root";
            var childKey = "child1";
            var query = CreateQueryWithChildren(rootKey, childKey);
            var context = CreateContextWithResults(query);

            var found = context.TryFindResult(k => k == childKey, out var resultKey);

            Assert.IsTrue(found);
            Assert.AreEqual(childKey, resultKey);
        }

        [TestMethod]
        public void TryFindResultReturnsFalseWhenNotFound()
        {
            var rootKey = "root";
            var query = CreateSimpleQuery(rootKey);
            var context = CreateContextWithResults(query);

            var found = context.TryFindResult(k => k == "nonexistent", out var resultKey);

            Assert.IsFalse(found);
            Assert.AreEqual(string.Empty, resultKey);
        }

        [TestMethod]
        public void TryFindResultFindsDeepNestedChild()
        {
            var rootKey = "root";
            var childKey1 = "child1";
            var grandchildKey = "grandchild1";
            
            var query = new NodeConnectionQuery("User", null, null, ConnectionArguments.Default, 25, true, null);
            var tree = ImmutableTree<string, GraphQueryNode>
                .Empty
                .AddNode(rootKey, new GraphQueryNode(query))
                .AddNode(childKey1, new GraphQueryNode(query))
                .AddNode(grandchildKey, new GraphQueryNode(query))
                .AddEdge(childKey1, rootKey)
                .AddEdge(grandchildKey, childKey1);
            
            var context = CreateContextWithResults(tree);

            var found = context.TryFindResult(k => k == grandchildKey, out var resultKey);

            Assert.IsTrue(found);
            Assert.AreEqual(grandchildKey, resultKey);
        }

        [TestMethod]
        public void TryFindResultSearchesMultipleChildrenWhenNoMatch()
        {
            var rootKey = "root";
            var childKey1 = "child1";
            var childKey2 = "child2";
            var childKey3 = "child3";
            var query = CreateQueryWithChildren(rootKey, childKey1, childKey2, childKey3);
            var context = CreateContextWithResults(query);

            // Search for a key that doesn't exist - this will iterate through all children
            var found = context.TryFindResult(k => k == "nonexistent", out var resultKey);

            Assert.IsFalse(found);
            Assert.AreEqual(string.Empty, resultKey);
        }

        #endregion
    }
}
