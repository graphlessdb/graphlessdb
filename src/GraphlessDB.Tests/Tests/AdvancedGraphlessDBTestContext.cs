/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System.Text.Json.Serialization;

namespace GraphlessDB.Tests
{
    [JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
    [JsonSerializable(typeof(Person))]
    [JsonSerializable(typeof(Role))]
    [JsonSerializable(typeof(PermissionNode))]
    [JsonSerializable(typeof(Concept))]
    [JsonSerializable(typeof(PropertyNode))]
    [JsonSerializable(typeof(Product))]
    [JsonSerializable(typeof(Server))]
    [JsonSerializable(typeof(Company))]
    [JsonSerializable(typeof(FriendshipEdge))]
    [JsonSerializable(typeof(DelegatesEdge))]
    [JsonSerializable(typeof(RoleInheritsRoleEdge))]
    [JsonSerializable(typeof(RoleHasPermissionEdge))]
    [JsonSerializable(typeof(IsAEdge))]
    [JsonSerializable(typeof(ConceptHasPropertyEdge))]
    [JsonSerializable(typeof(ComponentOfEdge))]
    [JsonSerializable(typeof(ParentOfEdge))]
    [JsonSerializable(typeof(NetworkLinkEdge))]
    [JsonSerializable(typeof(WorksAtEdge))]
    [JsonSerializable(typeof(ProducesEdge))]
    public partial class AdvancedGraphlessDBTestContext : JsonSerializerContext
    {
    }
}
