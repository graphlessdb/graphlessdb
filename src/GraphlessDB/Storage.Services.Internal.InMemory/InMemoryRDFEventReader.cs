/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using GraphlessDB.Threading;
using Microsoft.Extensions.Options;

namespace GraphlessDB.Storage.Services.Internal.InMemory
{
    internal sealed class InMemoryRDFEventReader(IOptions<GraphOptions> options) : IInMemoryRDFEventReader
    {
        private ImmutableList<RDFTriple> _events = [];
        private readonly Lock _locker = new();

        public void OnRDFTripleAdded(RDFTriple value)
        {
            if (value.Predicate.StartsWith(HasType.ByGraphName(options.Value.GraphName), StringComparison.Ordinal))
            {
                lock (_locker)
                {
                    _events = _events.Add(value);
                }
            }
        }
        public void OnRDFTripleUpdated(RDFTriple value)
        {
            if (value.Predicate.StartsWith(HasType.ByGraphName(options.Value.GraphName), StringComparison.Ordinal))
            {
                lock (_locker)
                {
                    _events = _events.Add(value);
                }
            }
        }

        public ImmutableList<RDFTriple> DequeueRDFTripleEvents()
        {
            lock (_locker)
            {
                var events = _events;
                _events = [];
                return events;
            }
        }
    }
}
