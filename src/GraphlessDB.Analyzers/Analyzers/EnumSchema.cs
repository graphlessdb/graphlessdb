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
    public sealed record EnumSchema(string Name, ImmutableListSequence<string> Values);
}
