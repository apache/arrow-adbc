/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Client
{
    /// <summary>
    /// Support for Arrow extension types in the ADO.NET client.
    /// </summary>
    internal static class ExtensionTypes
    {
        internal const string ExtensionNameKey = "ARROW:extension:name";
        internal const string ExtensionMetadataKey = "ARROW:extension:metadata";

        private static readonly DateTimeOffset s_unixEpoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

        /// <summary>
        /// The extension types the client understands without any registration by the caller.
        /// </summary>
        private static readonly ExtensionTypeRegistry s_builtIn = CreateBuiltInRegistry();

        private static ExtensionTypeRegistry CreateBuiltInRegistry()
        {
            ExtensionTypeRegistry registry = new ExtensionTypeRegistry();
            registry.Register(GuidExtensionDefinition.Instance);
            registry.Register(Bool8ExtensionDefinition.Instance);
            registry.Register(TimestampWithOffsetExtensionDefinition.Instance);
            registry.Register(VariantExtensionDefinition.Instance);
            return registry;
        }

        /// <summary>
        /// Arrays imported through the C Data Interface carry extension types only as field
        /// metadata over their storage type. Returns a field whose type is the corresponding
        /// <see cref="ExtensionType"/> when the extension is known, or the original field otherwise.
        /// Definitions registered with <see cref="ExtensionTypeRegistry.Default"/> take precedence
        /// over the built-in ones.
        /// </summary>
        public static Field Resolve(Field field)
        {
            if (field.DataType is ExtensionType ||
                !field.HasMetadata ||
                !field.Metadata.TryGetValue(ExtensionNameKey, out string? name))
            {
                return field;
            }

            if (!ExtensionTypeRegistry.Default.TryGetDefinition(name, out ExtensionDefinition definition) &&
                !s_builtIn.TryGetDefinition(name, out definition))
            {
                return field;
            }

            field.Metadata.TryGetValue(ExtensionMetadataKey, out string? metadata);
            if (!definition.TryCreateType(field.DataType, metadata ?? string.Empty, out ExtensionType extensionType))
            {
                return field;
            }

            return new Field(field.Name, extensionType, field.IsNullable, field.Metadata);
        }

        /// <summary>
        /// Gets a value converter for the given type, adding the client's handling of
        /// variants to the conversions supplied by <see cref="IArrowArrayExtensions"/>.
        /// </summary>
        public static Func<IArrowArray, int, object?> GetValueConverter(IArrowType type, StructBehavior structBehavior)
        {
            if (type is VariantType)
            {
                if (structBehavior == StructBehavior.JsonString)
                {
                    return (array, index) => array.IsNull(index) ? null : VariantJsonWriter.ToJson(GetVariantValue((VariantArray)array, index));
                }
                return (array, index) => array.IsNull(index) ? null : ToObject(GetVariantValue((VariantArray)array, index));
            }

            StructResultType structResultType = structBehavior == StructBehavior.JsonString ? StructResultType.JsonString : StructResultType.Object;
            return type.GetValueConverter(structResultType);
        }

        private static VariantValue GetVariantValue(VariantArray array, int index)
        {
            return array.IsShredded ? array.GetLogicalVariantValue(index) : array.GetVariantValue(index);
        }

        /// <summary>
        /// Converts a variant value to the equivalent .NET value. Objects become
        /// <see cref="Dictionary{TKey, TValue}"/> and arrays become <see cref="List{T}"/>.
        /// </summary>
        internal static object? ToObject(VariantValue value)
        {
            if (value.IsObject)
            {
                Dictionary<string, object?> result = new Dictionary<string, object?>();
                foreach (KeyValuePair<string, VariantValue> field in value.AsObject())
                {
                    result.Add(field.Key, ToObject(field.Value));
                }
                return result;
            }

            if (value.IsArray)
            {
                IReadOnlyList<VariantValue> elements = value.AsArray();
                List<object?> result = new List<object?>(elements.Count);
                foreach (VariantValue element in elements)
                {
                    result.Add(ToObject(element));
                }
                return result;
            }

            switch (value.PrimitiveType)
            {
                case VariantPrimitiveType.NullType:
                    return null;
                case VariantPrimitiveType.BooleanTrue:
                case VariantPrimitiveType.BooleanFalse:
                    return value.AsBoolean();
                case VariantPrimitiveType.Int8:
                    return value.AsInt8();
                case VariantPrimitiveType.Int16:
                    return value.AsInt16();
                case VariantPrimitiveType.Int32:
                    return value.AsInt32();
                case VariantPrimitiveType.Int64:
                    return value.AsInt64();
                case VariantPrimitiveType.Float:
                    return value.AsFloat();
                case VariantPrimitiveType.Double:
                    return value.AsDouble();
                case VariantPrimitiveType.Decimal4:
                case VariantPrimitiveType.Decimal8:
                    return value.AsDecimal();
                case VariantPrimitiveType.Decimal16:
                    return value.AsSqlDecimal();
                case VariantPrimitiveType.Date:
                    return value.AsDate();
                case VariantPrimitiveType.Timestamp:
                    return value.AsTimestamp();
                case VariantPrimitiveType.TimestampNtz:
                    return value.AsTimestampNtz();
                case VariantPrimitiveType.TimestampTzNanos:
                    return s_unixEpoch.AddTicks(value.AsTimestampTzNanos() / 100);
                case VariantPrimitiveType.TimestampNtzNanos:
                    return s_unixEpoch.UtcDateTime.AddTicks(value.AsTimestampNtzNanos() / 100);
                case VariantPrimitiveType.TimeNtz:
#if NET6_0_OR_GREATER
                    return new TimeOnly(value.AsTimeNtzMicros() * 10);
#else
                    return TimeSpan.FromTicks(value.AsTimeNtzMicros() * 10);
#endif
                case VariantPrimitiveType.String:
                    return value.AsString();
                case VariantPrimitiveType.Binary:
                    return value.AsBinary();
                case VariantPrimitiveType.Uuid:
                    return value.AsUuid();
                default:
                    throw new NotSupportedException($"Unsupported variant type: {value.PrimitiveType}");
            }
        }
    }
}
