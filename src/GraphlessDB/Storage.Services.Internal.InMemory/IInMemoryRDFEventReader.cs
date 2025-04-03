/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;

namespace GraphlessDB.Storage.Services.Internal.InMemory
{
    internal interface IInMemoryRDFEventReader
    {
        void OnRDFTripleAdded(RDFTriple value);

        void OnRDFTripleUpdated(RDFTriple value);

        ImmutableList<RDFTriple> DequeueRDFTripleEvents();
    }
}
