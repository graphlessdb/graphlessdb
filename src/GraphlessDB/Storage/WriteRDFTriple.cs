/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

namespace GraphlessDB.Storage
{
    public sealed record WriteRDFTriple(
        AddRDFTriple? Add,
        UpdateRDFTriple? Update,
        DeleteRDFTriple? Delete,
        UpdateRDFTripleAllEdgesVersion? UpdateAllEdgesVersion,
        IncrementRDFTripleAllEdgesVersion? IncrementAllEdgesVersion,
        CheckRDFTripleVersion? CheckRDFTripleVersion)
    {
        public static WriteRDFTriple Create(AddRDFTriple value)
        {
            return new WriteRDFTriple(value, null, null, null, null, null);
        }

        public static WriteRDFTriple Create(UpdateRDFTriple value)
        {
            return new WriteRDFTriple(null, value, null, null, null, null);
        }

        public static WriteRDFTriple Create(DeleteRDFTriple value)
        {
            return new WriteRDFTriple(null, null, value, null, null, null);
        }

        public static WriteRDFTriple Create(UpdateRDFTripleAllEdgesVersion value)
        {
            return new WriteRDFTriple(null, null, null, value, null, null);
        }

        public static WriteRDFTriple Create(IncrementRDFTripleAllEdgesVersion value)
        {
            return new WriteRDFTriple(null, null, null, null, value, null);
        }

        public static WriteRDFTriple Create(CheckRDFTripleVersion value)
        {
            return new WriteRDFTriple(null, null, null, null, null, value);
        }
    }
}
