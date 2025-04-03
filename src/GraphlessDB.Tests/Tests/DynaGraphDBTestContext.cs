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
    [JsonSerializable(typeof(Car))]
    [JsonSerializable(typeof(User))]
    [JsonSerializable(typeof(Manufacturer))]
    [JsonSerializable(typeof(UserOwnsCarEdge))]
    [JsonSerializable(typeof(UserLikesUserEdge))]
    [JsonSerializable(typeof(ManufacturerMakesCarEdge))]
    public partial class GraphlessDBTestContext : JsonSerializerContext
    {
    }
}
