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
        /// Fields nested in struct and list types are resolved too.
        /// Definitions registered with <see cref="ExtensionTypeRegistry.Default"/> take precedence
        /// over the built-in ones.
        /// </summary>
        public static Field Resolve(Field field)
        {
            if (field.DataType is ExtensionType)
            {
                return field;
            }

            IArrowType resolvedType = ResolveChildren(field.DataType);
            if (!ReferenceEquals(resolvedType, field.DataType))
            {
                field = new Field(field.Name, resolvedType, field.IsNullable, field.Metadata);
            }

            if (!field.HasMetadata ||
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

        private static IArrowType ResolveChildren(IArrowType type)
        {
            if (type is not NestedType nestedType)
            {
                return type;
            }

            Field[] fields = new Field[nestedType.Fields.Count];
            bool changed = false;
            for (int i = 0; i < fields.Length; i++)
            {
                fields[i] = Resolve(nestedType.Fields[i]);
                changed |= !ReferenceEquals(fields[i], nestedType.Fields[i]);
            }

            if (!changed)
            {
                return type;
            }

            switch (type)
            {
                case StructType:
                    return new StructType(fields);
                case ListType:
                    return new ListType(fields[0]);
                case LargeListType:
                    return new LargeListType(fields[0]);
                case FixedSizeListType fixedSizeListType:
                    return new FixedSizeListType(fields[0], fixedSizeListType.ListSize);
                default:
                    return type;
            }
        }

        /// <summary>
        /// Rebuilds an array whose type is the storage form of <paramref name="resolvedType"/>
        /// so that it has the resolved type, including extension arrays at any nesting level.
        /// The new array shares the original's buffers.
        /// </summary>
        public static IArrowArray Wrap(IArrowArray array, IArrowType resolvedType)
        {
            return ArrowArrayFactory.BuildArray(Retype(array.Data, resolvedType));
        }

        private static ArrayData Retype(ArrayData data, IArrowType type)
        {
            IArrowType storageType = type is ExtensionType extensionType ? extensionType.StorageType : type;
            ArrayData[] children = data.Children;
            if (storageType is NestedType nestedType && children != null && children.Length == nestedType.Fields.Count)
            {
                children = new ArrayData[children.Length];
                for (int i = 0; i < children.Length; i++)
                {
                    children[i] = Retype(data.Children[i], nestedType.Fields[i].DataType);
                }
            }

            return new ArrayData(type, data.Length, data.NullCount, data.Offset, data.Buffers, children, data.Dictionary);
        }
    }
}
