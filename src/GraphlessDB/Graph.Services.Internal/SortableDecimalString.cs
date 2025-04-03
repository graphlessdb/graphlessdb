/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Globalization;

namespace GraphlessDB.Graph.Services.Internal
{
    internal sealed class SortableDecimalString
    {
        private const ushort MidCharValue = ushort.MaxValue / 2;

        public static string ToString(decimal value)
        {
            return $"{EncodeSign(value)}{EncodeOrderOfMagnitude(value)}{EncodeValue(value)}";
        }

        public static decimal ToDecimal(string value)
        {
            return value[0] switch
            {
                '1' => decimal.Parse("-" + InvertString(value[2..]), CultureInfo.InvariantCulture),
                '2' => 0,
                _ => decimal.Parse(value[2..], CultureInfo.InvariantCulture)
            };
        }

        private static string EncodeSign(decimal value)
        {
            return Math.Sign(value) switch
            {
                -1 => "1",
                0 => "2",
                _ => "3"
            };
        }

        private static char EncodeOrderOfMagnitude(decimal value)
        {
            return Math.Sign(value) switch
            {
                -1 => (char)(MidCharValue - Math.Floor(Math.Log10(Convert.ToDouble(value) * -1))),
                0 => (char)MidCharValue,
                _ => (char)(MidCharValue + Math.Floor(Math.Log10(Convert.ToDouble(value))))
            };
        }

        private static string EncodeValue(decimal value)
        {
            return Math.Sign(value) switch
            {
                -1 => InvertString(value.ToString(CultureInfo.InvariantCulture)[1..]),
                0 => "0",
                _ => value.ToString(CultureInfo.InvariantCulture)
            };
        }

        private static string InvertString(string value)
        {
            var chars = value.ToCharArray();
            for (var i = 0; i < chars.Length; i++)
            {
                chars[i] = (char)('9' - chars[i] + '0' + 1);
            }

            return new string(chars);
        }
    }
}
