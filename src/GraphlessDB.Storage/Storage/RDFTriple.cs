/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Storage
{
#pragma warning disable CA1720
    public sealed record RDFTriple(
        string Subject,
        string Predicate,
        string IndexedObject,
        string @Object,
        string Partition,
        VersionDetail? VersionDetail);
#pragma warning restore CA1720
}