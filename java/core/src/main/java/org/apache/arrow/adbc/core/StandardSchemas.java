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
package org.apache.arrow.adbc.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public final class StandardSchemas {
  private StandardSchemas() {
    throw new AssertionError("Do not instantiate this class");
  }

  private static final ArrowType INT16 = Types.MinorType.SMALLINT.getType();
  private static final ArrowType INT32 = Types.MinorType.INT.getType();
  private static final ArrowType INT64 = Types.MinorType.BIGINT.getType();
  private static final ArrowType UINT32 = new ArrowType.Int(32, false);
  private static final ArrowType UINT64 = new ArrowType.Int(64, false);
  private static final ArrowType FLOAT64 =
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

  /** The schema of the result set of {@link AdbcConnection#getInfo(int[])}}. */
  public static final Schema GET_INFO_SCHEMA =
      new Schema(
          Arrays.asList(
              Field.notNullable("info_name", UINT32),
              new Field(
                  "info_value",
                  FieldType.nullable(
                      new ArrowType.Union(UnionMode.Dense, new int[] {0, 1, 2, 3, 4, 5})),
                  Arrays.asList(
                      Field.nullable("string_value", ArrowType.Utf8.INSTANCE),
                      Field.nullable("bool_value", ArrowType.Bool.INSTANCE),
                      Field.nullable("int64_value", INT64),
                      Field.nullable("int32_bitmask", INT32),
                      new Field(
                          "string_list",
                          FieldType.nullable(ArrowType.List.INSTANCE),
                          Collections.singletonList(
                              Field.nullable("item", ArrowType.Utf8.INSTANCE))),
                      new Field(
                          "int32_to_int32_list_map",
                          FieldType.nullable(new ArrowType.Map(/*keysSorted*/ false)),
                          Collections.singletonList(
                              new Field(
                                  "entries",
                                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                                  Arrays.asList(
                                      Field.notNullable("key", INT32),
                                      Field.nullable("value", INT32)))))))));

  /** The schema of the result set of {@link AdbcConnection#getTableTypes()}. */
  public static final Schema TABLE_TYPES_SCHEMA =
      new Schema(
          Collections.singletonList(Field.notNullable("table_type", ArrowType.Utf8.INSTANCE)));

  public static final List<Field> USAGE_SCHEMA =
      Arrays.asList(
          Field.nullable("fk_catalog", ArrowType.Utf8.INSTANCE),
          Field.nullable("fk_db_schema", ArrowType.Utf8.INSTANCE),
          Field.notNullable("fk_table", ArrowType.Utf8.INSTANCE),
          Field.notNullable("fk_column_name", ArrowType.Utf8.INSTANCE));

  public static final List<Field> CONSTRAINT_SCHEMA =
      Arrays.asList(
          Field.notNullable("constraint_name", ArrowType.Utf8.INSTANCE),
          Field.notNullable("constraint_type", ArrowType.Utf8.INSTANCE),
          new Field(
              "constraint_column_names",
              FieldType.nullable(ArrowType.List.INSTANCE),
              Collections.singletonList(Field.nullable("item", new ArrowType.Utf8()))),
          new Field(
              "constraint_column_usage",
              FieldType.nullable(ArrowType.List.INSTANCE),
              Collections.singletonList(
                  new Field("item", FieldType.nullable(ArrowType.Struct.INSTANCE), USAGE_SCHEMA))));

  public static final List<Field> COLUMN_SCHEMA =
      Arrays.asList(
          new Field(
              "column_name",
              FieldType.notNullable(ArrowType.Utf8.INSTANCE),
              Collections.emptyList()),
          new Field("ordinal_position", FieldType.nullable(INT32), Collections.emptyList()),
          new Field(
              "remarks", FieldType.nullable(ArrowType.Utf8.INSTANCE), Collections.emptyList()),
          new Field("xdbc_data_type", FieldType.nullable(INT16), Collections.emptyList()),
          new Field(
              "xdbc_type_name",
              FieldType.nullable(ArrowType.Utf8.INSTANCE),
              Collections.emptyList()),
          new Field("xdbc_column_size", FieldType.nullable(INT32), Collections.emptyList()),
          new Field("xdbc_decimal_digits", FieldType.nullable(INT16), Collections.emptyList()),
          new Field("xdbc_num_prec_radix", FieldType.nullable(INT16), Collections.emptyList()),
          new Field("xdbc_nullable", FieldType.nullable(INT16), Collections.emptyList()),
          new Field(
              "xdbc_column_def",
              FieldType.nullable(ArrowType.Utf8.INSTANCE),
              Collections.emptyList()),
          new Field("xdbc_sql_data_type", FieldType.nullable(INT16), Collections.emptyList()),
          new Field("xdbc_datetime_sub", FieldType.nullable(INT16), Collections.emptyList()),
          new Field("xdbc_char_octet_length", FieldType.nullable(INT32), Collections.emptyList()),
          new Field(
              "xdbc_is_nullable",
              FieldType.nullable(ArrowType.Utf8.INSTANCE),
              Collections.emptyList()),
          new Field(
              "xdbc_scope_catalog",
              FieldType.nullable(ArrowType.Utf8.INSTANCE),
              Collections.emptyList()),
          new Field(
              "xdbc_scope_schema",
              FieldType.nullable(ArrowType.Utf8.INSTANCE),
              Collections.emptyList()),
          new Field(
              "xdbc_scope_table",
              FieldType.nullable(ArrowType.Utf8.INSTANCE),
              Collections.emptyList()),
          new Field(
              "xdbc_is_autoincrement",
              FieldType.nullable(ArrowType.Bool.INSTANCE),
              Collections.emptyList()),
          new Field(
              "xdbc_is_generatedcolumn",
              FieldType.nullable(ArrowType.Bool.INSTANCE),
              Collections.emptyList()));

  public static final List<Field> TABLE_SCHEMA =
      Arrays.asList(
          new Field(
              "table_name",
              FieldType.notNullable(ArrowType.Utf8.INSTANCE),
              Collections.emptyList()),
          new Field(
              "table_type",
              FieldType.notNullable(ArrowType.Utf8.INSTANCE),
              Collections.emptyList()),
          new Field(
              "table_columns",
              FieldType.nullable(ArrowType.List.INSTANCE),
              Collections.singletonList(
                  new Field("item", FieldType.nullable(ArrowType.Struct.INSTANCE), COLUMN_SCHEMA))),
          new Field(
              "table_constraints",
              FieldType.nullable(ArrowType.List.INSTANCE),
              Collections.singletonList(
                  new Field(
                      "item", FieldType.nullable(ArrowType.Struct.INSTANCE), CONSTRAINT_SCHEMA))));

  public static final List<Field> DB_SCHEMA_SCHEMA =
      Arrays.asList(
          new Field(
              "db_schema_name",
              FieldType.notNullable(ArrowType.Utf8.INSTANCE),
              Collections.emptyList()),
          new Field(
              "db_schema_tables",
              FieldType.nullable(ArrowType.List.INSTANCE),
              Collections.singletonList(
                  new Field("item", FieldType.nullable(ArrowType.Struct.INSTANCE), TABLE_SCHEMA))));

  /**
   * The schema of the result of {@link AdbcConnection#getObjects(AdbcConnection.GetObjectsDepth,
   * String, String, String, String[], String)}.
   */
  public static final Schema GET_OBJECTS_SCHEMA =
      new Schema(
          Arrays.asList(
              new Field(
                  "catalog_name",
                  FieldType.notNullable(ArrowType.Utf8.INSTANCE),
                  Collections.emptyList()),
              new Field(
                  "catalog_db_schemas",
                  FieldType.nullable(ArrowType.List.INSTANCE),
                  Collections.singletonList(
                      new Field(
                          "item",
                          FieldType.nullable(ArrowType.Struct.INSTANCE),
                          DB_SCHEMA_SCHEMA)))));

  public static final List<Field> STATISTICS_VALUE_SCHEMA =
      Arrays.asList(
          Field.nullable("int64", INT64),
          Field.nullable("uint64", UINT64),
          Field.nullable("float64", FLOAT64),
          Field.nullable("binary", ArrowType.Binary.INSTANCE));

  public static final List<Field> STATISTICS_SCHEMA =
      Arrays.asList(
          Field.notNullable("table_name", ArrowType.Utf8.INSTANCE),
          Field.nullable("column_name", ArrowType.Utf8.INSTANCE),
          Field.notNullable("statistic_key", INT16),
          new Field(
              "statistic_value",
              FieldType.notNullable(new ArrowType.Union(UnionMode.Dense, new int[] {0, 1, 2, 3})),
              STATISTICS_VALUE_SCHEMA),
          Field.notNullable("statistic_is_approximate", ArrowType.Bool.INSTANCE));

  public static final List<Field> STATISTICS_DB_SCHEMA_SCHEMA =
      Arrays.asList(
          new Field(
              "db_schema_name",
              FieldType.notNullable(ArrowType.Utf8.INSTANCE),
              Collections.emptyList()),
          new Field(
              "db_schema_statistics",
              FieldType.notNullable(ArrowType.List.INSTANCE),
              Collections.singletonList(
                  new Field(
                      "item", FieldType.nullable(ArrowType.Struct.INSTANCE), STATISTICS_SCHEMA))));

  /**
   * The schema of the result of {@link AdbcConnection#getStatistics(String, String, String,
   * boolean)}.
   */
  public static final Schema GET_STATISTICS_SCHEMA =
      new Schema(
          Arrays.asList(
              new Field(
                  "catalog_name",
                  FieldType.notNullable(ArrowType.Utf8.INSTANCE),
                  Collections.emptyList()),
              new Field(
                  "catalog_db_schemas",
                  FieldType.notNullable(ArrowType.List.INSTANCE),
                  Collections.singletonList(
                      new Field(
                          "item",
                          FieldType.nullable(ArrowType.Struct.INSTANCE),
                          STATISTICS_DB_SCHEMA_SCHEMA)))));

  /** The schema of the result of {@link AdbcConnection#getStatisticNames()}. */
  public static final Schema GET_STATISTIC_NAMES_SCHEMA =
      new Schema(
          Arrays.asList(
              Field.notNullable("statistic_name", ArrowType.Utf8.INSTANCE),
              Field.notNullable("statistic_name", INT16)));
}
