/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

#pragma warning disable CS9113 // Parameter is unread.
#pragma warning disable CA1720 // Identifier contains type name
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
using GraphlessDB.Collections.Immutable;

namespace GraphlessDB.Analyzers
{

    public abstract record Entity(
        string Name,
        ImmutableListSequence<EntityProperty> Properties);
}
