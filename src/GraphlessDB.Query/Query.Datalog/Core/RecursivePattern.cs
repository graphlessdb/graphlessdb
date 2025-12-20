/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Domain.Graph;

namespace GraphlessDB.Query.Datalog.Core
{
    /// <summary>
    /// Pattern for recursive/transitive closure queries
    /// Example: Find all users connected through any number of "likes" edges
    /// </summary>
    public sealed record RecursivePattern(
        string Id,
        Variable<INode> StartVariable,
        Variable<INode> EndVariable,
        EdgePattern EdgePattern,
        int MinDepth,
        int MaxDepth,
        RecursionMode Mode)
    {
        /// <summary>
        /// Constant representing unbounded recursion depth
        /// </summary>
        public const int UnboundedDepth = -1;
    }

    /// <summary>
    /// Mode for recursive traversal
    /// </summary>
    public enum RecursionMode
    {
        /// <summary>
        /// Breadth-first search - explore all nodes at depth N before depth N+1
        /// </summary>
        BreadthFirst,

        /// <summary>
        /// Depth-first search - explore full path before backtracking
        /// </summary>
        DepthFirst
    }
}
