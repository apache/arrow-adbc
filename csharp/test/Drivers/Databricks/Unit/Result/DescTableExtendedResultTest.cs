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

using Apache.Arrow.Adbc.Drivers.Databricks.Result;
using System.Linq;
using System.Text.Json;

using static Apache.Arrow.Adbc.Drivers.Apache.Hive2.HiveServer2Connection;

using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit.Result
{
    public class DescTableExtendedResultTest
    {
        [Fact]
        public void TestTableWithoutConstraints()
        {
            var json = @"
            {
                ""table_name"": ""test_table"",
                ""catalog_name"": ""test_catalog"",
                ""schema_name"": ""test_schema"",
                ""columns"": [
                    {
                        ""name"": ""big_number"",
                        ""type"": {""name"": ""bigint""},
                        ""comment"": ""test comment"",
                        ""nullable"": false
                    },
                    {
                        ""name"": ""decimal_number"",
                        ""type"": {
                            ""name"": ""decimal"",
                            ""precision"": 10,
                            ""scale"": 2
                        },
                        ""nullable"": false
                    },
                    {
                    ""name"": ""string_col"",
                        ""type"": {
                            ""name"": ""string"",
                            ""collation"": ""UTF8_BINARY""
                        },
                        ""nullable"": false
                    },
                    {
                        ""name"": ""varchar_col"",
                        ""type"": {
                            ""name"": ""varchar"",
                            ""length"": 20
                        },
                        ""nullable"": false
                    },
                    {
                        ""name"": ""char_col"",
                        ""type"": {
                            ""name"": ""char"",
                            ""length"": 20
                        },
                        ""nullable"": false
                    },
                    {
                        ""name"": ""array_col"",
                        ""type"": {
                            ""name"": ""array"",
                            ""element_type"": {
                                ""name"": ""string"",
                                ""collation"": ""UTF8_BINARY""
                            },
                            ""element_nullable"": true
                        },
                        ""nullable"": true
                    },
                    {
                        ""name"": ""map_col"",
                        ""type"": {
                            ""name"": ""map"",
                            ""key_type"": {
                                ""name"": ""string"",
                                ""collation"": ""UTF8_BINARY""
                            },
                            ""value_type"": {
                                ""name"": ""int""
                            },
                            ""value_nullable"": true
                        },
                        ""nullable"": true
                    },
                    {
                        ""name"": ""interval_col"",
                        ""type"": {
                            ""name"": ""interval"",
                            ""start_unit"": ""year"",
                            ""end_unit"": ""month""
                        },
                        ""nullable"": true
                    },
                    {
                        ""name"": ""struct_col"",
                        ""type"": {
                            ""name"": ""struct"",
                            ""fields"": [
                                {
                                    ""name"": ""name"",
                                    ""type"": {
                                        ""name"": ""string"",
                                        ""collation"": ""UTF8_BINARY""
                                    },
                                    ""nullable"": true
                                },
                                {
                                    ""name"": ""age"",
                                    ""type"": {
                                        ""name"": ""int""
                                    },
                                    ""nullable"": true
                                },
                                {
                                    ""name"": ""scores"",
                                    ""type"": {
                                        ""name"": ""array"",
                                        ""element_type"": {
                                            ""name"": ""double""
                                        },
                                        ""element_nullable"": true
                                    },
                                    ""nullable"": true
                                }
                            ]
                        },
                        ""nullable"": true
                    }
                ],
                ""type"": ""MANAGED"",
                ""table_properties"": {
                    ""delta.minReaderVersion"": ""3""
                }
            }";

            var result = JsonSerializer.Deserialize<DescTableExtendedResult>(json);

            // Assert
            Assert.NotNull(result);
            Assert.Equal("test_table", result.TableName);
            Assert.Equal("test_catalog", result.CatalogName);
            Assert.Equal("test_schema", result.SchemaName);
            Assert.Equal("MANAGED", result.Type);

            Assert.Empty(result.PrimaryKeys);
            Assert.Empty(result.ForeignKeys);

            Assert.Equal(9, result.Columns.Count);

            var column = result.Columns.Find(c => c.Name == "big_number");
            Assert.NotNull(column);
            Assert.Equal("bigint", column.Type.Name);
            Assert.Equal("test comment", column.Comment);
            Assert.False(column!.Nullable);
            Assert.Equal("BIGINT", column.Type.FullTypeName);

            column = result.Columns.Find(c => c.Name == "decimal_number");
            Assert.NotNull(column);
            Assert.Equal("decimal", column.Type.Name);
            Assert.False(column.Nullable);
            Assert.Equal(10, column.Type.Precision);
            Assert.Equal(2, column.Type.Scale);
            Assert.Equal("DECIMAL(10,2)", column.Type.FullTypeName);

            column = result.Columns.Find(c => c.Name == "string_col");
            Assert.NotNull(column);
            Assert.Equal("string", column.Type.Name);
            Assert.Equal("STRING", column.Type.FullTypeName);

            column = result.Columns.Find(c => c.Name == "varchar_col");
            Assert.NotNull(column);
            Assert.Equal("varchar", column.Type.Name);
            Assert.Equal(20, column.Type.Length);
            Assert.Equal("VARCHAR(20)", column.Type.FullTypeName);

            column = result.Columns.Find(c => c.Name == "char_col");
            Assert.NotNull(column);
            Assert.Equal("char", column.Type.Name);
            Assert.Equal(20, column.Type.Length);
            Assert.Equal("CHAR(20)", column.Type.FullTypeName);

            column = result.Columns.Find(c => c.Name == "array_col");
            Assert.NotNull(column);
            Assert.Equal("array", column.Type.Name);
            Assert.NotNull(column.Type.ElementType);
            Assert.Equal("string", column.Type.ElementType.Name);
            Assert.Equal("ARRAY<STRING>", column.Type.FullTypeName);

            column = result.Columns.Find(c => c.Name == "map_col");
            Assert.NotNull(column);
            Assert.Equal("map", column.Type.Name);
            Assert.NotNull(column.Type.KeyType);
            Assert.NotNull(column.Type.ValueType);
            Assert.Equal("string", column.Type.KeyType.Name);
            Assert.Equal("int", column.Type.ValueType.Name);
            Assert.Equal("MAP<STRING, INT>", column.Type.FullTypeName);

            column = result.Columns.Find(c => c.Name == "interval_col");
            Assert.NotNull(column);
            Assert.Equal("interval", column.Type.Name);
            Assert.Equal("year", column.Type.StartUnit);
            Assert.Equal("month", column.Type.EndUnit);
            Assert.Equal("INTERVAL YEAR TO MONTH", column.Type.FullTypeName);

            column = result.Columns.Find(c => c.Name == "struct_col");
            Assert.NotNull(column);
            Assert.Equal("struct", column.Type.Name);
            Assert.NotNull(column.Type.Fields);
            Assert.Equal(3, column.Type.Fields.Count);
            Assert.Equal("STRUCT<name: STRING, age: INT, scores: ARRAY<DOUBLE>>", column.Type.FullTypeName);

            var field = column.Type.Fields.Find(c => c.Name == "name");
            Assert.NotNull(field);
            Assert.Equal("string", field.Type.Name);

            field = column.Type.Fields.Find(c => c.Name == "age");
            Assert.NotNull(field);
            Assert.Equal("int", field.Type.Name);

            field = column.Type.Fields.Find(c => c.Name == "scores");
            Assert.NotNull(field);
            Assert.Equal("array", field.Type.Name);
            Assert.Equal("double", field.Type.ElementType!.Name);
        }

        [SkippableTheory]
        [InlineData(
            "[(pk_id,PRIMARY KEY (`col1`,`col2`)),(fk_to_parent, FOREIGN KEY (`col3`,`col4`) REFERENCES `ref_catalog`.`ref_schema`.`ref_table` (`refcol1`, `refcol2`))]",
            new string[] { "col1", "col2" },
            "fk_to_parent",
            "ref_catalog.ref_schema.ref_table",
            new string[] { "col3", "col4" },
            new string[] { "refcol1", "refcol2" })]
        [InlineData(
            "[(pk_id,PRIMARY KEY (`col1`,`col2`)),(fk_to_parent, FOREIGN KEY (`col``3`,`col4`) REFERENCES `ref_catalog`.`ref_schema`.`ref_table` (`refcol``1`, `refcol2`))]",
            new string[] { "col1", "col2" },
            "fk_to_parent",
            "ref_catalog.ref_schema.ref_table",
            new string[] { "col`3", "col4" },
            new string[] { "refcol`1", "refcol2" })]
        [InlineData(
            "[(pk_id,PRIMARY KEY (`col1`,`col2`))]",
            new string[] { "col1", "col2" },
            "",
            "",
            new string[0],
            new string[0])]
        [InlineData(
            "[(pk_id,PRIMARY KEY (`col``1`,`col2`))]",
            new string[] { "col`1", "col2" },
            "",
            "",
            new string[0],
            new string[0])]
        [InlineData(
            "[(pk_id,PRIMARY KEY (`col,1(`,`col)2`))]",
            new string[] { "col,1(", "col)2" },
            "",
            "",
            new string[0],
            new string[0])]
        public void TestTableWithConstraints(string constraints, string[] primaryKeys, string fkName, string refTableName, string[] foreignKeys, string[] foreignRefKeys)
        {
            var json = @"
            {
                ""table_name"": ""test_table"",
                ""catalog_name"": ""test_catalog"",
                ""schema_name"": ""test_schema"",
                ""columns"": [
                    {
                        ""name"": ""id"",
                        ""type"": {
                            ""name"": ""int""
                        },
                        ""nullable"": false
                    }
                ],
                ""type"": ""MANAGED"",
                ""table_properties"": {
                    ""delta.minReaderVersion"": ""3""
                },
                ""table_constraints"": ""<constraints>""
            }".Replace("<constraints>", constraints);

            var result = JsonSerializer.Deserialize<DescTableExtendedResult>(json);
            var expectedPrimaryKeys = primaryKeys.ToList();

            Assert.NotNull(result);
            Assert.Equal("test_table", result.TableName);

            Assert.Equal(expectedPrimaryKeys, result.PrimaryKeys);

            if (string.IsNullOrEmpty(refTableName))
            {
                // No foreign key
                Assert.Empty(result.ForeignKeys);

            } else
            {
                var nameParts = refTableName.Split('.');
                var foreignKeynfo = result.ForeignKeys[0];
                Assert.Equal(fkName, foreignKeynfo.KeyName);
                Assert.Equal(foreignKeys.ToList(), foreignKeynfo.LocalColumns);
                Assert.Equal(foreignRefKeys.ToList(), foreignKeynfo.RefColumns);
                Assert.Equal(nameParts[0], foreignKeynfo.RefCatalog);

                Assert.Equal(nameParts[1], foreignKeynfo.RefSchema);
                Assert.Equal(nameParts[2], foreignKeynfo.RefTable);
            }
        }

        [SkippableTheory]
        [InlineData("BOOLEAN", ColumnTypeId.BOOLEAN, false, 1)]
        [InlineData("TINYINT", ColumnTypeId.TINYINT, true, 1)]
        [InlineData("BYTE", ColumnTypeId.TINYINT, true, 1)]
        [InlineData("SMALLINT", ColumnTypeId.SMALLINT, true, 2)]
        [InlineData("SHORT", ColumnTypeId.SMALLINT, true, 2)]
        [InlineData("INT", ColumnTypeId.INTEGER, true, 4)]
        [InlineData("INTEGER", ColumnTypeId.INTEGER, true, 4)]
        [InlineData("BIGINT", ColumnTypeId.BIGINT, true, 8)]
        [InlineData("LONG", ColumnTypeId.BIGINT, true, 8)]
        [InlineData("FLOAT", ColumnTypeId.FLOAT, true, 4)]
        [InlineData("REAL", ColumnTypeId.FLOAT, true, 4)]
        [InlineData("DOUBLE", ColumnTypeId.DOUBLE, true, 8)]
        [InlineData("DECIMAL", ColumnTypeId.DECIMAL, true, 11)]
        [InlineData("CHAR", ColumnTypeId.CHAR, false, 20)]
        [InlineData("VARCHAR", ColumnTypeId.VARCHAR, false, 20)]
        [InlineData("STRING", ColumnTypeId.VARCHAR, false, int.MaxValue)]
        [InlineData("BINARY", ColumnTypeId.BINARY,false, 0)]
        [InlineData("DATE", ColumnTypeId.DATE, false, 4)]
        [InlineData("TIMESTAMP", ColumnTypeId.TIMESTAMP, false, 8)]
        [InlineData("TIMESTAMP_LTZ", ColumnTypeId.TIMESTAMP, false, 8)]
        [InlineData("TIMESTAMP_NTZ", ColumnTypeId.TIMESTAMP, false, 8)]
        [InlineData("ARRAY", ColumnTypeId.ARRAY, false, 0)]
        [InlineData("MAP", ColumnTypeId.JAVA_OBJECT, false, 0)]
        [InlineData("STRUCT", ColumnTypeId.STRUCT, false, 0)]
        [InlineData("INTERVAL", ColumnTypeId.OTHER, false, 4, "YEAR")]
        [InlineData("INTERVAL", ColumnTypeId.OTHER, false, 8, "DAY")]
        [InlineData("VOID", ColumnTypeId.NULL, false, 1)]
        [InlineData("VARIANT", ColumnTypeId.OTHER, false, 0)]
        internal void TestCalculatedTypeProperties(string baseType, ColumnTypeId dataType, bool isNumber, int? columnSize, string? startUnit=null)
        {
            var json = @"
            {
                ""table_name"": ""test_table"",
                ""catalog_name"": ""test_catalog"",
                ""schema_name"": ""test_schema"",
                ""columns"": [
                    {
                        ""name"": ""col"",
                        ""type"": {
                            ""name"": ""<col_type>"",
                            ""length"": 20,
                            ""precision"": 11,
                            ""start_unit"": <start_unit>
                        },
                        ""nullable"": false
                    }
                ],
                ""type"": ""MANAGED"",
                ""table_properties"": {
                    ""delta.minReaderVersion"": ""3""
                }
            }".Replace("<col_type>", baseType)
              .Replace("<start_unit>", startUnit != null ? $"\"{startUnit}\"" : "null");

            var result = JsonSerializer.Deserialize<DescTableExtendedResult>(json);
            var column = result!.Columns[0];

            Assert.Equal(dataType, column.DataType);
            Assert.Equal(isNumber, column.IsNumber);
            Assert.Equal(columnSize, column.ColumnSize);
        }
    }
}
