/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using GraphlessDB;
using GraphlessDB.Storage;

namespace GraphlessDB.Graph.Services.Internal
{
    internal sealed class RDFTripleFactory(
        IGraphSettingsService graphOptionsProvider,
        IGraphPartitionService partitionMapper,
        IGraphEntityTypeService typeMapper,
        IGraphQueryablePropertyService queryablePropertyService,
        IGraphEntitySerializationService entitySerializer,
        IGraphSerializationService entityValueSerializer) : IRDFTripleFactory
    {
        public RDFTriple HasType(INode node)
        {
            var options = graphOptionsProvider.GetGraphSettings();
            var json = entitySerializer.SerializeNode(node, node.GetType());
            return new RDFTriple(
                node.Id,
                new HasType(options.GraphName, node.GetType().Name, node.Id).ToString(),
                " ",
                json,
                partitionMapper.GetPartition(node.Id),
                node.Version);
        }

        public RDFTriple HasBlob(INode node)
        {
            var options = graphOptionsProvider.GetGraphSettings();
            var json = entitySerializer.SerializeNode(node, node.GetType());
            return new RDFTriple(
                node.Id,
                new HasBlob(options.GraphName, node.GetType().Name, node.Version.NodeVersion).ToString(),
                " ",
                json,
                partitionMapper.GetPartition(node.Id),
                null);
        }

        public RDFTriple HasProp(INode node, string propertyName, string propertyValue)
        {
            // NOTE : The indexed value cannot be an empty string, we must replace it.  Is this appropriate ???
            var indexedPropertyValue = Truncate(propertyValue, 100);
            if (indexedPropertyValue == string.Empty)
            {
                indexedPropertyValue = " ";
            }

            var options = graphOptionsProvider.GetGraphSettings();
            return new RDFTriple(
                node.Id,
                new HasProp(options.GraphName, node.GetType().Name, propertyName, Truncate(propertyValue, 100), node.Id).ToString(),
                indexedPropertyValue,
                propertyValue,
                partitionMapper.GetPartition(node.Id),
                null);
        }

        public RDFTriple HasInEdge(IEdge edge)
        {
            var options = graphOptionsProvider.GetGraphSettings();
            var json = entitySerializer.SerializeEdge(edge, edge.GetType());
            return new RDFTriple(
                edge.InId,
                new HasInEdge(options.GraphName, GlobalId.Parse(edge.InId).TypeName, edge.GetType().Name, edge.InId, edge.OutId).ToString(),
                " ",
                json,
                partitionMapper.GetPartition(edge.InId),
                null);
        }

        public RDFTriple HasInEdgeProp(IEdge edge, string propertyName, string propertyValue)
        {
            // NOTE : The indexed value cannot be an empty string, we must replace it.  Is this appropriate ???
            var indexedPropertyValue = Truncate(propertyValue, 100);
            if (indexedPropertyValue == string.Empty)
            {
                indexedPropertyValue = " ";
            }

            var options = graphOptionsProvider.GetGraphSettings();
            return new RDFTriple(
                edge.InId,
                new HasInEdgeProp(
                    options.GraphName,
                    GlobalId.Parse(edge.InId).TypeName,
                    edge.GetType().Name,
                    propertyName,
                    Truncate(propertyValue, 100),
                    edge.InId,
                    edge.OutId).ToString(),
                indexedPropertyValue,
                propertyValue,
                partitionMapper.GetPartition(edge.InId),
                null);
        }

        public RDFTriple HadInEdge(IEdge edge)
        {
            var options = graphOptionsProvider.GetGraphSettings();
            var json = entitySerializer.SerializeEdge(edge, edge.GetType());
            return new RDFTriple(
                edge.InId,
                new HadInEdge(options.GraphName, GlobalId.Parse(edge.InId).TypeName, edge.GetType().Name, edge.InId, edge.OutId, edge.CreatedAt, edge.DeletedAt).ToString(),
                " ",
                json,
                partitionMapper.GetPartition(edge.InId),
                null);
        }

        public RDFTriple HasOutEdge(IEdge edge)
        {
            var options = graphOptionsProvider.GetGraphSettings();
            var json = entitySerializer.SerializeEdge(edge, edge.GetType());
            return new RDFTriple(
                edge.OutId,
                new HasOutEdge(options.GraphName, GlobalId.Parse(edge.OutId).TypeName, edge.GetType().Name, edge.InId, edge.OutId).ToString(),
                " ",
                json,
                partitionMapper.GetPartition(edge.OutId),
                null);
        }

        public RDFTriple HasOutEdgeProp(IEdge edge, string propertyName, string propertyValue)
        {
            // NOTE : The indexed value cannot be an empty string, we must replace it.  Is this appropriate ???
            var indexedPropertyValue = Truncate(propertyValue, 100);
            if (indexedPropertyValue == string.Empty)
            {
                indexedPropertyValue = " ";
            }

            var options = graphOptionsProvider.GetGraphSettings();
            return new RDFTriple(
                edge.OutId,
                new HasOutEdgeProp(
                    options.GraphName,
                    GlobalId.Parse(edge.OutId).TypeName,
                    edge.GetType().Name,
                    propertyName,
                    Truncate(propertyValue, 100),
                    edge.InId,
                    edge.OutId).ToString(),
                indexedPropertyValue,
                propertyValue,
                partitionMapper.GetPartition(edge.OutId),
                null);
        }

        public RDFTriple HadOutEdge(IEdge edge)
        {
            var options = graphOptionsProvider.GetGraphSettings();
            var json = entitySerializer.SerializeEdge(edge, edge.GetType());
            return new RDFTriple(
                edge.OutId,
                new HadOutEdge(options.GraphName, GlobalId.Parse(edge.OutId).TypeName, edge.GetType().Name, edge.OutId, edge.InId, edge.CreatedAt, edge.DeletedAt).ToString(),
                " ",
                json,
                partitionMapper.GetPartition(edge.OutId),
                null);
        }

        public INode GetNode(RDFTriple item)
        {
            if (item.VersionDetail == null)
            {
                throw new GraphlessDBOperationException();
            }

            var type = typeMapper.GetEntityType(GlobalId.Parse(item.Subject).TypeName);
            var value = entitySerializer.DeserializeNode(item.Object, type);
            value = value with { Version = item.VersionDetail };
            return value;
        }

        public IEdge GetEdge(RDFTriple item)
        {
            return entitySerializer.DeserializeEdge(item.Object, GetEdgeType(item));
        }

        private Type GetEdgeType(RDFTriple item)
        {
            if (Storage.HasInEdge.IsPredicate(item.Predicate))
            {
                var predicate = Storage.HasInEdge.Parse(item.Predicate);
                return typeMapper.GetEntityType(predicate.EdgeTypeName);
            }

            if (Storage.HasOutEdge.IsPredicate(item.Predicate))
            {
                var predicate = Storage.HasOutEdge.Parse(item.Predicate);
                return typeMapper.GetEntityType(predicate.EdgeTypeName);
            }

            throw new GraphlessDBOperationException();
        }

        public RDFTriple GetHasTypeRDFTriple(INode node)
        {
            return HasType(node);
        }

        public RDFTriple GetHasBlobRDFTriple(INode node)
        {
            return HasBlob(node);
        }

        public ImmutableList<RDFTriple> GetHasEdgeRDFTriples(IEdge edge)
        {
            return [HasInEdge(edge), HasOutEdge(edge)];
        }

        [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2075:DynamicallyAccessedMembersAttribute")]
        public ImmutableList<RDFTriple> GetHasEdgePropRDFTriples(IEdge edge)
        {
            return edge
                .GetType()
                .GetProperties()
                .Where(prop => StoreEdgeProp(edge.GetType(), prop) && prop.GetValue(edge) != null)
                .SelectMany(prop => new[] {
                    HasInEdgeProp(
                        edge,
                        prop.Name,
                        entityValueSerializer.GetPropertyAsString(prop.GetValue(edge))),
                    HasOutEdgeProp(
                        edge,
                        prop.Name,
                        entityValueSerializer.GetPropertyAsString(prop.GetValue(edge)))})
                .ToImmutableList();
        }

        private bool StoreEdgeProp(Type type, PropertyInfo prop)
        {
            return prop.Name switch
            {
                nameof(IEdge.DeletedAt) or nameof(IEdge.InId) or nameof(IEdge.OutId) => false,
                _ => queryablePropertyService.IsQueryableProperty(type.Name, prop.Name),
            };
        }

        private bool StoreNodeProp(Type type, PropertyInfo prop)
        {
            // NOTE Currently if stored, these values will remain after deletion because 
            // the deleted object values for these does not match the data in the database
            // and thus the update (delete) does not work correctly.  It makes sense that 
            // they are not stored anyway.
            return prop.Name switch
            {
                nameof(INode.DeletedAt) or nameof(INode.Version) => false,
                _ => queryablePropertyService.IsQueryableProperty(type.Name, prop.Name),
            };
        }

        public ImmutableList<RDFTriple> GetHadEdgeRDFTriples(IEdge edge)
        {
            return [HadInEdge(edge), HadOutEdge(edge)];
        }

        [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2075:DynamicallyAccessedMembersAttribute")]
        public ImmutableList<RDFTriple> GetHasPropRDFTriples(INode node)
        {
            return node
                .GetType()
                .GetProperties()
                .Where(prop => StoreNodeProp(node.GetType(), prop) && prop.GetValue(node) != null)
                .Select(prop => HasProp(
                        node,
                        prop.Name,
                        entityValueSerializer.GetPropertyAsString(prop.GetValue(node))))
                .ToImmutableList();
        }

        private static string Truncate(string value, int length)
        {
            return value.Length <= length ? value : value[..length];
        }
    }
}