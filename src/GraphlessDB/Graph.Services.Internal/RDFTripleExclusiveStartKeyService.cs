/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Linq;
using GraphlessDB;
using GraphlessDB.Storage;

namespace GraphlessDB.Graph.Services.Internal
{
    internal sealed class RDFTripleExclusiveStartKeyService(
        IGraphSettingsService graphSettingsService,
        IGraphCursorSerializationService cursorSerializer,
        IGraphSerializationService entityValueSerializer) : IRDFTripleExclusiveStartKeyService
    {
        public HasTypeCursor? TryGetHasTypeCursor(ConnectionArguments page)
        {
            if (page == null || page.After == null && page.Before == null)
            {
                return default;
            }

            var cursorText = page.After ??
                page.Before ??
                throw new GraphlessDBOperationException("Cursor was missing");

            var cursor = cursorSerializer.Deserialize(cursorText);
            var cursorItem = cursor.GetRootNodeOrDefault();
            if (cursorItem?.HasType == null)
            {
                throw new NotSupportedException("Cursor type not supported");
            }

            return cursorItem.HasType;
        }

        public RDFTripleKeyWithPartition? TryGetHasTypeExclusiveStartKey(ConnectionArguments? page, int queryIndex, string typeName)
        {
            if (page == null || page.After == null && page.Before == null)
            {
                return default;
            }

            var options = graphSettingsService.GetGraphSettings();

            var cursorText = page.After ??
                page.Before ??
                throw new GraphlessDBOperationException("Cursor was missing");

            var cursor = cursorSerializer.Deserialize(cursorText);
            var cursorItem = cursor.GetRootNodeOrDefault();
            if (cursorItem?.HasType != null)
            {
                var queryCursor = cursorItem.HasType.QueryCursors[queryIndex];
                if (queryCursor.Position == PartitionPosition.Start)
                {
                    return default;
                }

                if (queryCursor.Position == PartitionPosition.Finish)
                {
                    throw new GraphlessDBOperationException("Cursor Position Finished not expected");
                }

                if (queryCursor.Position != PartitionPosition.Cursor || queryCursor.Cursor == null)
                {
                    throw new GraphlessDBOperationException("Cursor for partition not found");
                }

                return new RDFTripleKeyWithPartition(
                    queryCursor.Cursor.Subject,
                    new HasType(options.GraphName, typeName, queryCursor.Cursor.Subject).ToString(),
                    queryCursor.Cursor.Partition);
            }

            throw new NotSupportedException("Cursor type not supported");
        }

        public RDFTripleKey? TryGetHasEdgeExclusiveStartKey(ConnectionArguments? connectionArguments, GraphSettings options, ToEdgeQueryRequest request)
        {
            if (connectionArguments == null || connectionArguments.After == null && connectionArguments.Before == null)
            {
                return default;
            }

            if (request.EdgeTypeName == null)
            {
                throw new GraphlessDBOperationException("Edge type name was expected");
            }

            var cursorText = connectionArguments.After ?? connectionArguments.Before ?? throw new GraphlessDBOperationException("Cursor was missing");
            var cursor = cursorSerializer.Deserialize(cursorText);
            var cursorItem = cursor.GetRootNodeOrDefault();
            if (cursorItem?.HasInEdge != null)
            {
                return new RDFTripleKey(
                    cursorItem.HasInEdge.Subject,
                    new HasInEdge(
                        options.GraphName,
                        request.NodeTypeName,
                        request.EdgeTypeName,
                        cursorItem.HasInEdge.Subject,
                        cursorItem.HasInEdge.NodeOutId).ToString());
            }

            if (cursorItem?.HasInEdgeProp != null)
            {
                return new RDFTripleKey(
                    cursorItem.HasInEdgeProp.Subject,
                    new HasInEdgeProp(
                        options.GraphName,
                        request.NodeTypeName,
                        request.EdgeTypeName,
                        request.FilterBy?.PropertyName ?? request.OrderBy?.PropertyName ?? throw new GraphlessDBOperationException("Expected property name"),
                        cursorItem.HasInEdgeProp.PropertyValue,
                        cursorItem.HasInEdgeProp.Subject,
                        cursorItem.HasInEdgeProp.NodeOutId).ToString());
            }

            if (cursorItem?.HasOutEdge != null)
            {
                return new RDFTripleKey(
                    cursorItem.HasOutEdge.Subject,
                    new HasOutEdge(
                        options.GraphName,
                        request.NodeTypeName,
                        request.EdgeTypeName,
                        cursorItem.HasOutEdge.NodeInId,
                        cursorItem.HasOutEdge.Subject).ToString());
            }

            if (cursorItem?.HasOutEdgeProp != null)
            {
                return new RDFTripleKey(
                    cursorItem.HasOutEdgeProp.Subject,
                    new HasOutEdgeProp(
                        options.GraphName,
                        request.NodeTypeName,
                        request.EdgeTypeName,
                        request.FilterBy?.PropertyName ?? request.OrderBy?.PropertyName ?? throw new GraphlessDBOperationException("Expected property name"),
                        cursorItem.HasOutEdgeProp.PropertyValue,
                        cursorItem.HasOutEdgeProp.NodeInId,
                        cursorItem.HasOutEdgeProp.Subject).ToString());
            }

            if (cursorItem != null && cursorItem.EndOfData != null)
            {
                return default;
            }

            throw new NotSupportedException("Cursor type not supported");
        }

        public HasPropCursor? TryGetHasPropCursor(ConnectionArguments page)
        {
            if (page.After == null && page.Before == null)
            {
                return default;
            }

            var cursorText = page.After ?? page.Before ?? throw new GraphlessDBOperationException("Cursor was missing");
            var cursor = cursorSerializer.Deserialize(cursorText);
            var cursorItem = cursor.GetRootNodeOrDefault();
            return cursorItem?.HasProp == null ? throw new NotSupportedException("Cursor type not supported") : cursorItem.HasProp;
        }

        public RDFTripleKeyWithPartition? TryGetPropertiesByTypeAndPropertyNameExclusiveStartKey(ConnectionArguments page, int queryIndex, string typeName, string propertyName)
        {
            if (page.After == null && page.Before == null)
            {
                return default;
            }

            var options = graphSettingsService.GetGraphSettings();
            var hasPropCursor = TryGetHasPropCursor(page) ?? throw new NotSupportedException("Cursor type not supported");
            var hasPropCursorForPartition = hasPropCursor
                .QueryCursors
                .Where(p => p.Index == queryIndex)
                .Single();

            if (hasPropCursorForPartition.Position == PartitionPosition.Start)
            {
                return default;
            }

            if (hasPropCursorForPartition.Position == PartitionPosition.Finish)
            {
                throw new GraphlessDBOperationException("Cursor Position Finished not expected");
            }

            var partitionCursor = hasPropCursorForPartition.Cursor ?? throw new GraphlessDBOperationException("Null partition cursor not expected");
            var encodedPropertyValue = entityValueSerializer.GetPropertyAsString(partitionCursor.PropertyValue);
            return new RDFTripleKeyWithPartition(
                partitionCursor.Subject,
                new HasProp(options.GraphName, typeName, propertyName, encodedPropertyValue, partitionCursor.Subject).ToString(),
                partitionCursor.Partition);
        }

        public RDFTripleKeyWithPartition? TryGetPropertiesByTypePropertyNameAndValueExclusiveStartKey(ConnectionArguments page, int queryIndex, string typeName, string propertyName)
        {
            if (page.After == null && page.Before == null)
            {
                return default;
            }

            var options = graphSettingsService.GetGraphSettings();
            var hasPropCursor = TryGetHasPropCursor(page) ?? throw new NotSupportedException("Cursor type not supported");
            var hasPropCursorForPartition = hasPropCursor
                .QueryCursors
                .Where(p => p.Index == queryIndex)
                .Single();

            if (hasPropCursorForPartition.Position == PartitionPosition.Start)
            {
                return default;
            }

            if (hasPropCursorForPartition.Position == PartitionPosition.Finish)
            {
                throw new GraphlessDBOperationException("Cursor Position Finished not expected");
            }

            var partitionCursor = hasPropCursorForPartition.Cursor ?? throw new GraphlessDBOperationException("Null partition cursor not expected");
            var encodedPropertyValue = entityValueSerializer.GetPropertyAsString(partitionCursor.PropertyValue);
            return new RDFTripleKeyWithPartition(
                partitionCursor.Subject,
                new HasProp(options.GraphName, typeName, propertyName, encodedPropertyValue, partitionCursor.Subject).ToString(),
                partitionCursor.Partition);
        }
    }
}