/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.Collections.Immutable;

namespace GraphlessDB.DynamoDB.Transactions.Storage
{
    // An immutable class that can be used in map keys.  Does a copy of the attribute value
    // to prevent any member from being mutated.
    public sealed record ImmutableAttributeValue(
        string? S,
        string? N,
        ImmutableListSequence<byte>? B,
        ImmutableListSequence<RecordTuple<string, ImmutableAttributeValue>>? M,
        ImmutableListSequence<string>? NS,
        ImmutableListSequence<string>? SS,
        ImmutableListSequence<ImmutableListSequence<byte>>? BS)
    {
        public static ImmutableAttributeValue Create(AttributeValue value)
        {
            return new ImmutableAttributeValue(
                value.S,
                value.N,
                value.B?.ToArray().ToImmutableList().ToImmutableListSequence(),
                value.IsMSet ? value.M.OrderBy(v => v.Key).Select(v => new RecordTuple<string, ImmutableAttributeValue>(v.Key, Create(v.Value))).ToImmutableList().ToImmutableListSequence() : null,
                value.NS != null && value.NS.Count > 0 ? value.NS.ToImmutableList().ToImmutableListSequence() : null,
                value.SS != null && value.SS.Count > 0 ? value.SS?.ToImmutableList().ToImmutableListSequence() : null,
                value.BS != null && value.BS.Count > 0 ? value.BS?.Select(b => b.ToArray().ToImmutableList().ToImmutableListSequence()).ToImmutableList().ToImmutableListSequence() : null);
        }

        public AttributeValue ToAttributeValue()
        {
            if (S != null)
            {
                return AttributeValueFactory.CreateS(S);
            }

            if (N != null)
            {
                return AttributeValueFactory.CreateN(N);
            }

            if (B != null)
            {
                return AttributeValueFactory.CreateB(new MemoryStream([.. B.Items]));
            }

            if (M != null)
            {
                return AttributeValueFactory.CreateM(M.Items.ToDictionary(k => k.Item1, v => v.Item2.ToAttributeValue()));
            }

            if (NS != null)
            {
                return AttributeValueFactory.CreateNS([.. NS.Items]);
            }

            if (SS != null)
            {
                return AttributeValueFactory.CreateSS([.. SS.Items]);
            }

            if (BS != null)
            {
                return AttributeValueFactory.CreateBS(BS.Items.Select(b => new MemoryStream([.. b.Items])).ToList());
            }

            throw new InvalidOperationException();
        }
    }
}