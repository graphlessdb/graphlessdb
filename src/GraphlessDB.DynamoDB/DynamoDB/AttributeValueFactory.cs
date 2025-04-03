/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Generic;
using System.IO;
using Amazon.DynamoDBv2.Model;

namespace GraphlessDB.DynamoDB
{
    public static class AttributeValueFactory
    {
        public static AttributeValue CreateS(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                throw new ArgumentException("Must not be null or empty", nameof(s));
            }

            return new AttributeValue(s);
        }

        public static AttributeValue CreateN(string n)
        {
            if (string.IsNullOrWhiteSpace(n))
            {
                throw new ArgumentException("Must not be null, empty or whitespace", nameof(n));
            }

            return new AttributeValue { N = n };
        }

        public static AttributeValue CreateBOOL(bool value)
        {
            return new AttributeValue { BOOL = value };
        }

        public static AttributeValue CreateNULL(bool value)
        {
            return new AttributeValue { NULL = value };
        }

        public static AttributeValue CreateB(MemoryStream b)
        {
            ArgumentNullException.ThrowIfNull(b);
            return new AttributeValue
            {
                B = b
            };
        }

        public static AttributeValue CreateBS(List<MemoryStream> bs)
        {
            ArgumentNullException.ThrowIfNull(bs);

            if (bs.Count == 0)
            {
                throw new ArgumentException("Must be one or more items", nameof(bs));
            }

            return new AttributeValue
            {
                BS = bs
            };
        }

        public static AttributeValue CreateNS(List<string> ns)
        {
            ArgumentNullException.ThrowIfNull(ns);

            if (ns.Count == 0)
            {
                throw new ArgumentException("Must be one or more items", nameof(ns));
            }

            return new AttributeValue
            {
                NS = ns
            };
        }

        public static AttributeValue CreateSS(List<string> ss)
        {
            ArgumentNullException.ThrowIfNull(ss);

            if (ss.Count == 0)
            {
                throw new ArgumentException("Must be one or more items", nameof(ss));
            }

            return new AttributeValue
            {
                SS = ss
            };
        }

        public static AttributeValue CreateL(List<AttributeValue> l)
        {
            ArgumentNullException.ThrowIfNull(l);

            if (l.Count == 0)
            {
                throw new ArgumentException("Must be one or more items", nameof(l));
            }

            return new AttributeValue
            {
                L = l
            };
        }

        public static AttributeValue CreateM(Dictionary<string, AttributeValue> m)
        {
            ArgumentNullException.ThrowIfNull(m);

            if (m.Count == 0)
            {
                throw new ArgumentException("Must be one or more items", nameof(m));
            }

            return new AttributeValue
            {
                M = m
            };
        }
    }
}
