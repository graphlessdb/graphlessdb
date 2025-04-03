/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using GraphlessDB.Collections.Immutable;

namespace GraphlessDB.Analyzers
{
    public sealed record GraphSchema(
        string Namespace,
        string Name,
        ImmutableListSequence<EnumSchema> Enums,
        ImmutableListSequence<Entity> Entities)
    {
        public static readonly GraphSchema Empty = new(
            string.Empty,
            string.Empty,
            ImmutableListSequence<EnumSchema>.Empty,
            ImmutableListSequence<Entity>.Empty);
    }
}
