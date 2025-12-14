/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Generic;
using System.Globalization;
using Amazon.DynamoDBv2.Model;
using GraphlessDB.DynamoDB;

namespace GraphlessDB.Storage.Services.DynamoDB
{
    internal sealed class AmazonDynamoDBRDFTripleItemService : IAmazonDynamoDBRDFTripleItemService
    {
        public Dictionary<string, AttributeValue> ToAttributeMap(RDFTripleKey key)
        {
            return new Dictionary<string, AttributeValue> {
                { nameof(RDFTriple.Subject), AttributeValueFactory.CreateS(key.Subject) },
                { nameof(RDFTriple.Predicate), AttributeValueFactory.CreateS(key.Predicate) },
            };
        }

        public Dictionary<string, AttributeValue> ToAttributeMap(RDFTripleKeyWithPartition key)
        {
            return new Dictionary<string, AttributeValue> {
                { nameof(RDFTriple.Subject), AttributeValueFactory.CreateS(key.Subject) },
                { nameof(RDFTriple.Predicate), AttributeValueFactory.CreateS(key.Predicate) },
                { nameof(RDFTriple.Partition), AttributeValueFactory.CreateS(key.Partition) },
            };
        }

        public Dictionary<string, AttributeValue> ToAttributeMap(RDFTriple value)
        {
            var attributeMap = new Dictionary<string, AttributeValue>{
                { nameof(RDFTriple.Subject), AttributeValueFactory.CreateS(value.Subject) },
                { nameof(RDFTriple.Predicate), AttributeValueFactory.CreateS(value.Predicate) },
                { nameof(RDFTriple.IndexedObject), AttributeValueFactory.CreateS(value.IndexedObject) },
                { nameof(RDFTriple.Object), AttributeValueFactory.CreateS(value.Object) },
                { nameof(RDFTriple.Partition), AttributeValueFactory.CreateS(value.Partition) },
            };

            if (value.VersionDetail != null)
            {
                attributeMap.Add(
                    nameof(RDFTriple.VersionDetail),
                    AttributeValueFactory.CreateM(GetAttributeMap(value.VersionDetail)));
            }

            return attributeMap;
        }

        public bool IsRDFTriple(Dictionary<string, AttributeValue> value)
        {
            return value.ContainsKey(nameof(RDFTriple.Subject)) &&
                value.ContainsKey(nameof(RDFTriple.Predicate)) &&
                value.ContainsKey(nameof(RDFTriple.Object)) &&
                value.ContainsKey(nameof(RDFTriple.Partition));
        }

        public RDFTriple ToRDFTriple(Dictionary<string, AttributeValue> value)
        {
            value.TryGetValue(nameof(RDFTriple.VersionDetail), out var versionDetailAttribute);
            value.TryGetValue(nameof(RDFTriple.IndexedObject), out var indexedObjectAttribute);
            return new RDFTriple(
                value[nameof(RDFTriple.Subject)].S,
                value[nameof(RDFTriple.Predicate)].S,
                indexedObjectAttribute?.S ?? " ",
                value[nameof(RDFTriple.Object)].S,
                value[nameof(RDFTriple.Partition)].S,
                GetVersionDetail(versionDetailAttribute?.M));
        }

        private static Dictionary<string, AttributeValue> GetAttributeMap(VersionDetail value)
        {
            var map = new Dictionary<string, AttributeValue> {
                { nameof(VersionDetail.NodeVersion), AttributeValueFactory.CreateN(value.NodeVersion.ToString(CultureInfo.InvariantCulture)) },
                { nameof(VersionDetail.AllEdgesVersion), AttributeValueFactory.CreateN(value.AllEdgesVersion.ToString(CultureInfo.InvariantCulture)) }
            };

            return map;
        }

        private static VersionDetail? GetVersionDetail(Dictionary<string, AttributeValue>? value)
        {
            if (value == null)
            {
                return null;
            }

            return new VersionDetail(
                int.Parse(value[nameof(VersionDetail.NodeVersion)].N, CultureInfo.InvariantCulture),
                int.Parse(value[nameof(VersionDetail.AllEdgesVersion)].N, CultureInfo.InvariantCulture));
        }
    }
}