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

using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// The standard schemas
    /// </summary>
    public static class StandardSchemas
    {
        /// <summary>
        /// The schema of the result set of
        /// <see cref="AdbcConnection.GetInfo(int[])"/>}.
        /// </summary>
        public static readonly Schema GetInfoSchema =
            new Schema(
                new Field[]
                {
                    new Field("info_name", UInt32Type.Default, false),
                    new Field(
                        "info_value",
                        new UnionType(
                            new Field[]
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
                                            new Field[]
                                            {
                                                new Field("key", Int32Type.Default, false),
                                                new Field("value", Int32Type.Default, true),
                                            }
                                            ), false)
                                    ),
                                    true
                                )
                            },
                            // TBD if this line is the best approach but its a good one-liner
                            new int[] {0, 1, 2, 3, 4, 5}.ToArray(),
                            UnionMode.Dense),
                        true)
                },
                metadata: null
        );

        public static readonly Schema TableTypesSchema = new Schema(
            new Field[]
            {
                new Field("table_type", StringType.Default, false)
            },
            metadata: null
        );

        public static readonly IReadOnlyList<Field> UsageSchema = new Field[]
        {
            new Field("fk_catalog", StringType.Default, true),
            new Field("fk_db_schema", StringType.Default, true),
            new Field("fk_table", StringType.Default, false),
            new Field("fk_column_name", StringType.Default, false)
        };

        public static readonly IReadOnlyList<Field> ConstraintSchema = new Field[]
        {
            new Field("constraint_name", StringType.Default, false),
            new Field("constraint_type", StringType.Default, false),
            new Field("constraint_column_names",
                new ListType(
                    new Field("item", StringType.Default, true)
                ),
                false
            ),
            new Field("constraint_column_usage",
                new ListType(
                    new Field("item", new StructType(UsageSchema), true)
                ),
                false
            ),
        };

        public static readonly IReadOnlyList<Field> ColumnSchema =
            new Field[]
            {
                new Field("column_name", StringType.Default, false),
                new Field("ordinal_position", Int32Type.Default, true),
                new Field("remarks", StringType.Default, true),
                new Field("xdbc_data_type", Int16Type.Default, true),
                new Field("xdbc_type_name", StringType.Default, true),
                new Field("xdbc_column_size", Int32Type.Default, true),
                new Field("xdbc_decimal_digits", Int16Type.Default, true),
                new Field("xdbc_num_prec_radix", Int16Type.Default, true),
                new Field("xdbc_nullable", Int16Type.Default, true),
                new Field("xdbc_column_def", StringType.Default, true),
                new Field("xdbc_sql_data_type", Int16Type.Default, true),
                new Field("xdbc_datetime_sub", Int16Type.Default, true),
                new Field("xdbc_char_octet_length", Int32Type.Default, true),
                new Field("xdbc_is_nullable", StringType.Default, true),
                new Field("xdbc_scope_catalog", StringType.Default, true),
                new Field("xdbc_scope_schema", StringType.Default, true),
                new Field("xdbc_scope_table", StringType.Default, true),
                new Field("xdbc_is_autoincrement", BooleanType.Default, true),
                new Field("xdbc_is_generatedcolumn", BooleanType.Default, true)
            };

        public static readonly IReadOnlyList<Field> TableSchema = new Field[] {
          new Field("table_name", StringType.Default, false, null),
          new Field("table_type", StringType.Default, false, null),
          new Field(
              "table_columns",
              new ListType(
                new Field("item", new StructType(ColumnSchema), true)
              ),
              false
          ),
          new Field(
              "table_constraints",
              new ListType(
                new Field("item", new StructType(ConstraintSchema), true)
              ),
              false
          )
        };

        public static readonly IReadOnlyList<Field> DbSchemaSchema = new Field[]
        {
            new Field("db_schema_name", StringType.Default, false, null),
            new Field(
                "db_schema_tables",
                new ListType(
                    new Field("item", new StructType(TableSchema), true)
                ),
                false
            )
        };

        public static readonly Schema GetObjectsSchema = new Schema(
            new Field[]
            {
                new Field("catalog_name", StringType.Default, false),
                new Field(
                    "catalog_db_schemas",
                    new ListType(
                        new Field("item", new StructType(DbSchemaSchema), true)
                    ),
                    false
                )
            },
            metadata: null
        );
    }
}
