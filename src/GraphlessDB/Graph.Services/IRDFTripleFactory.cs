/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Immutable;
using GraphlessDB.Storage;

namespace GraphlessDB.Graph.Services
{
    public interface IRDFTripleFactory
    {
        INode GetNode(RDFTriple item);

        IEdge GetEdge(RDFTriple item);

        RDFTriple HasType(INode node);

        RDFTriple HasBlob(INode node);

        RDFTriple HasProp(INode node, string propertyName, string propertyValue);

        RDFTriple HasInEdge(IEdge edge);

        RDFTriple HasInEdgeProp(IEdge edge, string propertyName, string propertyValue);

        RDFTriple HasOutEdge(IEdge edge);

        RDFTriple HasOutEdgeProp(IEdge edge, string propertyName, string propertyValue);

        RDFTriple HadInEdge(IEdge edge);

        RDFTriple HadOutEdge(IEdge edge);

        RDFTriple GetHasTypeRDFTriple(INode node);

        RDFTriple GetHasBlobRDFTriple(INode node);

        ImmutableList<RDFTriple> GetHasEdgeRDFTriples(IEdge edge);

        ImmutableList<RDFTriple> GetHasEdgePropRDFTriples(IEdge edge);

        ImmutableList<RDFTriple> GetHadEdgeRDFTriples(IEdge edge);

        ImmutableList<RDFTriple> GetHasPropRDFTriples(INode node);
    }
}