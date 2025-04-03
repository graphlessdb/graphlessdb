/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Linq;

namespace GraphlessDB
{
    public static class StringFilterExtensions
    {
        public static bool IsMatch(this StringFilter source, string? value)
        {
            if (source.Eq != null && value != source.Eq)
            {
                return false;
            }

            if (source.Ne != null && value == source.Ne)
            {
                return false;
            }

            if (source.Le != null && string.CompareOrdinal(value, source.Le) > 0)
            {
                return false;
            }

            if (source.Lt != null && string.CompareOrdinal(value, source.Lt) <= 0)
            {
                return false;
            }

            if (source.Ge != null && string.CompareOrdinal(value, source.Ge) < 0)
            {
                return false;
            }

            if (source.Gt != null && string.CompareOrdinal(value, source.Gt) <= 0)
            {
                return false;
            }

            if (source.Contains != null && value != null && !value.Contains(source.Contains))
            {
                return false;
            }

            if (source.NotContains != null && value != null && value.Contains(source.NotContains))
            {
                return false;
            }

            if (source.Between != null)
            {
                throw new NotSupportedException("Between is not currently supported");
            }

            if (source.BeginsWith != null && value != null && !value.StartsWith(source.BeginsWith, StringComparison.Ordinal))
            {
                return false;
            }

            if (source.BeginsWithAny != null && value != null && !source.BeginsWithAny.Where(f => value.StartsWith(f, StringComparison.Ordinal)).Any())
            {
                return false;
            }

            if (source.In != null && !source.In.Contains(value))
            {
                return false;
            }

            return true;
        }

        public static StringFilter? ToLowerCase(this StringFilter value)
        {
            if (value == null)
            {
                return null;
            }
            return new StringFilter
            {
                Eq = value.Eq?.ToLowerInvariant(),
                Ne = value.Ne?.ToLowerInvariant(),
                Le = value.Le?.ToLowerInvariant(),
                Lt = value.Lt?.ToLowerInvariant(),
                Ge = value.Ge?.ToLowerInvariant(),
                Gt = value.Gt?.ToLowerInvariant(),
                Contains = value.Contains?.ToLowerInvariant(),
                NotContains = value.NotContains?.ToLowerInvariant(),
                Between = ToLowerCase(value.Between),
                BeginsWith = value.BeginsWith?.ToLowerInvariant(),
                BeginsWithAny = ToLowerCase(value.BeginsWithAny),
                In = ToLowerCase(value.In),
            };
        }

        private static StringRange? ToLowerCase(StringRange? value)
        {
            if (value == null)
            {
                return null;
            }

            return new StringRange(value.Min?.ToLowerInvariant(), value.Max?.ToLowerInvariant());
        }

        private static string[]? ToLowerCase(string[]? value)
        {
            if (value == null)
            {
                return null;
            }

            return [.. value.Select(v => v.ToLowerInvariant())];
        }
    }
}
