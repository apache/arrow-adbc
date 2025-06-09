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

using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Result
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

            Assert.Equal(6, result.Columns.Count);

            var column = result.Columns.Find(c => c.Name == "big_number");
            Assert.NotNull(column);
            Assert.Equal("bigint", column.Type.Name);
            Assert.False(column!.Nullable);

            column = result.Columns.Find(c => c.Name == "decimal_number");
            Assert.NotNull(column);
            Assert.Equal("decimal", column.Type.Name);
            Assert.False(column.Nullable);
            Assert.Equal(10, column.Type.Precision);
            Assert.Equal(2, column.Type.Scale);

            column = result.Columns.Find(c => c.Name == "string_col");
            Assert.NotNull(column);
            Assert.Equal("string", column.Type.Name);

            column = result.Columns.Find(c => c.Name == "array_col");
            Assert.NotNull(column);
            Assert.Equal("array", column.Type.Name);
            Assert.NotNull(column.Type.ElementType);
            Assert.Equal("string", column.Type.ElementType.Name);

            column = result.Columns.Find(c => c.Name == "map_col");
            Assert.NotNull(column);
            Assert.Equal("map", column.Type.Name);
            Assert.NotNull(column.Type.KeyType);
            Assert.NotNull(column.Type.ValueType);
            Assert.Equal("string", column.Type.KeyType.Name);
            Assert.Equal("int", column.Type.ValueType.Name);

            column = result.Columns.Find(c => c.Name == "struct_col");
            Assert.NotNull(column);
            Assert.Equal("struct", column.Type.Name);
            Assert.NotNull(column.Type.Fields);
            Assert.Equal(3, column.Type.Fields.Count);

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
            "[(pk_id,PRIMARY KEY (`col1`,`col2`)),(fk_constraint, FOREIGN KEY (`col3`,`col4`) REFERENCES `ref_catalog`.`ref_schema`.`ref_table` (`refcol1`, `refcol2`))]",
            new string[] { "col1", "col2" },
            "ref_catalog.ref_schema.ref_table",
            new string[] { "col3", "col4" },
            new string[] { "refcol1", "refcol2" })]
        [InlineData(
            "[(pk_id,PRIMARY KEY (`col1`,`col2`)),(fk_constraint, FOREIGN KEY (`col``3`,`col4`) REFERENCES `ref_catalog`.`ref_schema`.`ref_table` (`refcol``1`, `refcol2`))]",
            new string[] { "col1", "col2" },
            "ref_catalog.ref_schema.ref_table",
            new string[] { "col`3", "col4" },
            new string[] { "refcol`1", "refcol2" })]
        [InlineData(
            "[(pk_id,PRIMARY KEY (`col1`,`col2`))]",
            new string[] { "col1", "col2" },
            "",
            new string[0],
            new string[0])]
        [InlineData(
            "[(pk_id,PRIMARY KEY (`col``1`,`col2`))]",
            new string[] { "col`1", "col2" },
            "",
            new string[0],
            new string[0])]
        [InlineData(
            "[(pk_id,PRIMARY KEY (`col,1(`,`col)2`))]",
            new string[] { "col,1(", "col)2" },
            "",
            new string[0],
            new string[0])]
        public void TestTableWithConstraints(string constaints, string[] primaryKeys, string refTableName, string[] foreignKeys, string[] foreignRefKeys)
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
                ""table_constraints"": ""<constaints>""
            }".Replace("<constaints>", constaints);

            var result = JsonSerializer.Deserialize<DescTableExtendedResult>(json);
            var expectedPrimaryKeys = primaryKeys.ToList();
           
            Assert.NotNull(result);
            Assert.Equal("test_table", result.TableName);

            Assert.Equal(expectedPrimaryKeys, result.PrimaryKeys);

            if (string.IsNullOrEmpty(refTableName))
            {
                // No foreing key
                Assert.Empty(result.ForeignKeys);

            } else
            {
                var nameParts = refTableName.Split('.');
                var foreignKeynfo = result.ForeignKeys[0];
                Assert.Equal(foreignKeys.ToList(), foreignKeynfo.LocalColumns);
                Assert.Equal(foreignRefKeys.ToList(), foreignKeynfo.RefColumns);
                Assert.Equal(nameParts[0], foreignKeynfo.RefCatalog);

                Assert.Equal(nameParts[1], foreignKeynfo.RefSchema);
                Assert.Equal(nameParts[2], foreignKeynfo.RefTable);
            }
        }
    }
}
