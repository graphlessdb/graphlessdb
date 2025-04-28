/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Text;

namespace GraphlessDB
{
    public sealed record GlobalId(string TypeName, string Id)
    {
        public static string Get<T>(string id)
        {
            var globalId = new GlobalId(typeof(T).Name, id);
            return globalId.ToString();
        }

        public static string ParseId<T>(string value)
        {
            var (typeName, id) = Parse(value);
            if (typeName != typeof(T).Name)
            {
                throw new ArgumentException("Global id type mismatch");
            }

            return id;
        }

        public static GlobalId Parse(string value)
        {
            var parts = Encoding.UTF8.GetString(Convert.FromBase64String(value)).Split('#');
            if (parts.Length != 2)
            {
                throw new ArgumentException("Could not parse global id");
            }

            return new GlobalId(parts[0], parts[1]);
        }

        public override string ToString()
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes($"{TypeName}#{Id}"));
        }
    }
}
