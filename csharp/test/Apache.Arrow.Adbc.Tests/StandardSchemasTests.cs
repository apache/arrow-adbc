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
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Validate StandardSchema.
    /// </summary>
    public class StandardSchemasTests
    {
        [Fact]
        public void CanValidateSchema()
        {
            IReadOnlyList<IArrowArray> columnDataArrays = StandardSchemas.ColumnSchema.Validate(GetColumnDataArrays());
            IReadOnlyList<IArrowArray> tableDataArrays = StandardSchemas.TableSchema.Validate(GetTableDataArrays(columnDataArrays));
            IReadOnlyList<IArrowArray> schemaDataArrays = StandardSchemas.DbSchemaSchema.Validate(GetDbSchemaDataArrays(tableDataArrays));
            _ = StandardSchemas.GetObjectsSchema.Validate(GetGetObjectsDataArrays(schemaDataArrays));
        }

        [Fact]
        public void CanInvalidateColumnTypeSchema()
        {
            Assert.Throws<ArgumentException>(() => StandardSchemas.ColumnSchema.Validate(GetColumnDataArraysInvalidColumnType()));
        }

        [Fact]
        public void CanInvalidateColumnCountSchema()
        {
            Assert.Throws<ArgumentException>(() => StandardSchemas.ColumnSchema.Validate(GetColumnDataArraysMissingColumn()));
        }

        [Fact]
        public void CanInvalidateTableSchema()
        {
            IReadOnlyList<IArrowArray> columnDataArrays = GetColumnDataArraysInvalidColumnType();
            Assert.Throws<ArgumentException>(() => StandardSchemas.TableSchema.Validate(GetTableDataArrays(columnDataArrays)));
        }

        [Fact]
        public void CanInvalidateDbSchema()
        {
            IReadOnlyList<IArrowArray> columnDataArrays = GetColumnDataArraysInvalidColumnType();
            IReadOnlyList<IArrowArray> tableDataArrays = GetTableDataArrays(columnDataArrays);
            Assert.Throws<ArgumentException>(() => StandardSchemas.DbSchemaSchema.Validate(GetDbSchemaDataArrays(tableDataArrays)));
        }

        [Fact]
        public void CanInvalidateGetObjectsSchema()
        {
            IReadOnlyList<IArrowArray> columnDataArrays = GetColumnDataArraysInvalidColumnType();
            IReadOnlyList<IArrowArray> tableDataArrays = GetTableDataArrays(columnDataArrays);
            IReadOnlyList<IArrowArray?> schemaDataArrays = GetDbSchemaDataArrays(tableDataArrays);
            Assert.Throws<ArgumentException>(() => StandardSchemas.GetObjectsSchema.Validate(GetGetObjectsDataArrays(schemaDataArrays)));
        }

        [Fact]
        public void CanValidateGetInfoSchema()
        {
            _ = StandardSchemas.GetInfoSchema.Validate(GetGetInfoDataArrays());
        }

        [Fact]
        public void CanInvalidateGetInfoSchema()
        {
            Exception exception = Assert.Throws<ArgumentException>(() => StandardSchemas.GetInfoSchema.Validate(GetGetInfoDataArraysWithInvalidType()));
            Assert.Contains("Expecting data type Apache.Arrow.Types.StringType but found Apache.Arrow.Types.Int32Type on field with name item.", exception.Message);
        }

        private IReadOnlyList<IArrowArray> GetGetObjectsDataArrays(IReadOnlyList<IArrowArray?> schemaDataArrays)
        {
            List<IArrowArray?> catalogDbSchemasValues = new List<IArrowArray?>()
            {
                new StructArray(
                    new StructType(StandardSchemas.DbSchemaSchema),
                    0,
                    schemaDataArrays,
                    new ArrowBuffer.BitmapBuilder().Build())
            };
            IReadOnlyList<IArrowArray> getObjectsDataArrays = new List<IArrowArray>
            {
                new StringArray.Builder().Build(),
                catalogDbSchemasValues.BuildListArrayForType(new StructType(StandardSchemas.DbSchemaSchema)),
            };
            return getObjectsDataArrays;
        }

        private IReadOnlyList<IArrowArray> GetDbSchemaDataArrays(IReadOnlyList<IArrowArray> tableDataArrays)
        {
            List<IArrowArray> dbSchemaTablesValues = new List<IArrowArray>()
            {
                new StructArray(
                    new StructType(StandardSchemas.TableSchema),
                    0,
                    tableDataArrays,
                    new ArrowBuffer.BitmapBuilder().Build())
            };

            List<IArrowArray> schemaDataArrays = new List<IArrowArray>
            {
                new StringArray.Builder().Build(),
                dbSchemaTablesValues.BuildListArrayForType(new StructType(StandardSchemas.TableSchema)),
            };

            return schemaDataArrays;
        }

        private IReadOnlyList<IArrowArray> GetTableDataArrays(IReadOnlyList<IArrowArray> columnDataArrays)
        {
            var columnData = new StructArray(
                new StructType(StandardSchemas.ColumnSchema),
                0,
                columnDataArrays,
                new ArrowBuffer.BitmapBuilder().Build());
            List<IArrowArray?> tableColumnsValues = new List<IArrowArray?>()
            {
                columnData,
            };
            List<IArrowArray?> tableConstraintsValues = new List<IArrowArray?>()
            {
                null,
            };
            List<IArrowArray> tableDataArrays = new List<IArrowArray>
            {
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                tableColumnsValues.BuildListArrayForType(new StructType(StandardSchemas.ColumnSchema)),
                tableConstraintsValues.BuildListArrayForType(new StructType(StandardSchemas.ConstraintSchema))
            };
            return tableDataArrays;
        }

        private static List<IArrowArray> GetColumnDataArrays() => new List<IArrowArray>
            {
                new StringArray.Builder().Build(),
                new Int32Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int16Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int32Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new BooleanArray.Builder().Build(),
                new BooleanArray.Builder().Build(),
            };

        private static List<IArrowArray> GetColumnDataArraysInvalidColumnType() => new List<IArrowArray>
            {
                new StringArray.Builder().Build(),
                new Int32Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int16Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int32Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new BooleanArray.Builder().Build(),
                new StringArray.Builder().Build(), // invalid type, expects BooleanArray
            };

        private static List<IArrowArray> GetColumnDataArraysMissingColumn() => new List<IArrowArray>
            {
                new StringArray.Builder().Build(),
                new Int32Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int16Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int32Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int16Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new BooleanArray.Builder().Build(),
                // new BooleanArray.Builder().Build(), // missing column
            };

        private static List<IArrowArray> GetGetInfoDataArrays()
        {
            UnionType infoUnionType = new UnionType(
                new List<Field>()
                {
                    new Field("string_value", StringType.Default, true),
                    new Field("bool_value", BooleanType.Default, true),
                    new Field("int64_value", Int64Type.Default, true),
                    new Field("int32_bitmask", Int32Type.Default, true),
                    new Field(
                        "string_list",
                        new ListType(
                            new Field("item", StringType.Default, true)
                        ),
                        false
                    ),
                    new Field(
                        "int32_to_int32_list_map",
                        new ListType(
                            new Field("entries", new StructType(
                                new List<Field>()
                                {
                                    new Field("key", Int32Type.Default, false),
                                    new Field("value", Int32Type.Default, true),
                                }
                                ), false)
                        ),
                        true
                    )
                },
                new int[] { 0, 1, 2, 3, 4, 5 }.ToArray(),
                UnionMode.Dense);

            UInt32Array.Builder infoNameBuilder = new UInt32Array.Builder();
            ArrowBuffer.Builder<byte> typeBuilder = new ArrowBuffer.Builder<byte>();
            ArrowBuffer.Builder<int> offsetBuilder = new ArrowBuffer.Builder<int>();
            StringArray.Builder stringInfoBuilder = new StringArray.Builder();
            int nullCount = 0;
            int arrayLength = 0;


            StructType entryType = new StructType(
                new List<Field>(){
                    new Field("key", Int32Type.Default, false),
                    new Field("value", Int32Type.Default, true)});

            StructArray entriesDataArray = new StructArray(entryType, 0,
                new[] { new Int32Array.Builder().Build(), new Int32Array.Builder().Build() },
                new ArrowBuffer.BitmapBuilder().Build());

            List<IArrowArray> childrenArrays = new List<IArrowArray>()
            {
                stringInfoBuilder.Build(),
                new BooleanArray.Builder().Build(),
                new Int64Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new ListArray.Builder(StringType.Default).Build(),
                new List<IArrowArray?>(){ entriesDataArray }.BuildListArrayForType(entryType)
            };

            DenseUnionArray infoValue = new DenseUnionArray(infoUnionType, arrayLength, childrenArrays, typeBuilder.Build(), offsetBuilder.Build(), nullCount);

            List<IArrowArray> dataArrays = new List<IArrowArray>
            {
                infoNameBuilder.Build(),
                infoValue
            };
            return dataArrays;
        }

        private static List<IArrowArray> GetGetInfoDataArraysWithInvalidType()
        {
            UnionType infoUnionType = new UnionType(
                new List<Field>()
                {
                    new Field("string_value", StringType.Default, true),
                    new Field("bool_value", BooleanType.Default, true),
                    new Field("int64_value", Int64Type.Default, true),
                    new Field("int32_bitmask", Int32Type.Default, true),
                    new Field(
                        "string_list",
                        new ListType(
                            new Field("item", StringType.Default, true)
                        ),
                        false
                    ),
                    new Field(
                        "int32_to_int32_list_map",
                        new ListType(
                            new Field("entries", new StructType(
                                new List<Field>()
                                {
                                    new Field("key", Int32Type.Default, false),
                                    new Field("value", Int32Type.Default, true),
                                }
                                ), false)
                        ),
                        true
                    )
                },
                new int[] { 0, 1, 2, 3, 4, 5 }.ToArray(),
                UnionMode.Dense);

            UInt32Array.Builder infoNameBuilder = new UInt32Array.Builder();
            ArrowBuffer.Builder<byte> typeBuilder = new ArrowBuffer.Builder<byte>();
            ArrowBuffer.Builder<int> offsetBuilder = new ArrowBuffer.Builder<int>();
            StringArray.Builder stringInfoBuilder = new StringArray.Builder();
            int nullCount = 0;
            int arrayLength = 0;


            StructType entryType = new StructType(
                new List<Field>(){
                    new Field("key", Int32Type.Default, false),
                    new Field("value", Int32Type.Default, true)});

            StructArray entriesDataArray = new StructArray(
                entryType,
                0,
                new[] { new Int32Array.Builder().Build(), new Int32Array.Builder().Build() },
                new ArrowBuffer.BitmapBuilder().Build());

            List<IArrowArray> childrenArrays = new List<IArrowArray>()
            {
                stringInfoBuilder.Build(),
                new BooleanArray.Builder().Build(),
                new Int64Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new ListArray.Builder(Int32Type.Default).Build(), // Should be StringType.Default
                new List<IArrowArray?>(){ entriesDataArray }.BuildListArrayForType(entryType)
            };

            DenseUnionArray infoValue = new DenseUnionArray(infoUnionType, arrayLength, childrenArrays, typeBuilder.Build(), offsetBuilder.Build(), nullCount);

            List<IArrowArray> dataArrays = new List<IArrowArray>
            {
                infoNameBuilder.Build(),
                infoValue
            };
            return dataArrays;
        }
    }
}
