/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace GraphlessDB.DynamoDB
{
    public sealed class FailingAmazonDynamoDBOptions
    {
        public FailingAmazonDynamoDBOptions()
        {
            RequestsToFail = [];
            GetRequestsToTreatAsDeleted = [];
            GetRequestsToStub = [];
        }

        // Any requests added to this set will throw a FailedYourRequestException when called.
        public HashSet<AmazonWebServiceRequest> RequestsToFail { get; }

        // Any requests added to this set will return a null item when called
        public HashSet<GetItemRequest> GetRequestsToTreatAsDeleted { get; }

        // Any requests with keys in this set will return the queue of responses in order. When the end of the queue is reached
        // further requests will be passed to the DynamoDB client.
        public Dictionary<GetItemRequest, Queue<GetItemResponse>> GetRequestsToStub { get; }
    }
}
