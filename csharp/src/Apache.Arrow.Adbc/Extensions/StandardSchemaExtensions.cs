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
using System.Linq;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Extensions
{
    public static class StandardSchemaExtensions
    {

        /// <summary>
        /// Validates a data array that its column number and types match a given schema.
        /// </summary>
        /// <param name="schema">The schema to validate against.</param>
        /// <param name="data">The data array to validate.</param>
        /// <exception cref="ArgumentException">Throws an exception if the number of columns or type data types in the data array do not match the schema fields.</exception>
        public static IReadOnlyList<IArrowArray> Validate(this Schema schema, IReadOnlyList<IArrowArray> data)
        {
            Validate(schema.FieldsList, data);
            return data;
        }

        /// <summary>
        /// Validates a data array that its column number and types match given schema fields.
        /// </summary>
        /// <param name="schemaFields">The schema fields to validate against.</param>
        /// <param name="data">The data array to validate.</param>
        /// <exception cref="ArgumentException">Throws an exception if the number of columns or type data types in the data array do not match the schema fields.</exception>
        public static IReadOnlyList<IArrowArray> Validate(this IReadOnlyList<Field> schemaFields, IReadOnlyList<IArrowArray> data)
        {
            if (schemaFields.Count != data.Count)
            {
                throw new ArgumentException($"Expected number of columns {schemaFields.Count} not equal to actual length {data.Count}", nameof(data));
            }
            for (int i = 0; i < schemaFields.Count; i++)
            {
                Field field = schemaFields[i];
                ArrayData dataField = data[i].Data;

                if (field.DataType.TypeId != dataField.DataType.TypeId)
                {
                    throw new ArgumentException($"Expecting data type {field.DataType} but found {data[i].Data.DataType} on field with name {field.Name}.", nameof(data));
                }
                if (field.DataType.TypeId == ArrowTypeId.Struct)
                {
                    StructType structType = (StructType)field.DataType;
                    Validate(structType.Fields, dataField.Children.Select(e => new ContainerArray(e)).ToList());
                }
                else if (field.DataType.TypeId == ArrowTypeId.List)
                {
                    ListType listType = (ListType)field.DataType;
                    int j = 0;
                    Field f = listType.Fields[j];

                    List<Field> fieldsToValidate = new List<Field>();
                    List<ContainerArray> arrayDataToValidate = new List<ContainerArray>();

                    ArrayData? child = j < dataField.Children.Length ? dataField.Children[j] : null;

                    if (child != null)
                    {
                        fieldsToValidate.Add(f);
                        arrayDataToValidate.Add(new ContainerArray(child));
                    }
                    else if (!f.IsNullable)
                    {
                        throw new InvalidOperationException("Received a null value for a non-nullable field");
                    }

                    Validate(fieldsToValidate, arrayDataToValidate);
                }
                else if (field.DataType.TypeId == ArrowTypeId.Union)
                {
                    UnionType unionType = (UnionType)field.DataType;
                    if (unionType.Fields.Count > 0)
                    {
                        Validate(unionType.Fields, dataField.Children.Select(e => new ContainerArray(e)).ToList());
                    }
                }
            }

            return data;
        }

        private class ContainerArray : Array
        {
            public ContainerArray(ArrayData data) : base(data)
            {
            }
        }
    }
}
