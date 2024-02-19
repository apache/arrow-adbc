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

package org.apache.arrow.adbc.driver.testsuite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.core.StandardSchemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Common tests of metadata methods of AdbcConnection. */
public abstract class AbstractConnectionMetadataTest {
  /** Must be initialized by the subclass. */
  protected static SqlValidationQuirks quirks;

  protected AdbcDatabase database;
  protected AdbcConnection connection;
  protected BufferAllocator allocator;
  protected SqlTestUtil util;
  protected String tableName;
  protected String mainTable;
  protected String dependentTable;

  @BeforeEach
  public void beforeEach() throws Exception {
    Preconditions.checkNotNull(quirks, "Must initialize quirks in subclass with @BeforeAll");
    allocator = new RootAllocator();
    database = quirks.initDatabase(allocator);
    connection = database.connect();
    util = new SqlTestUtil(quirks);
    tableName = quirks.caseFoldTableName("foo");
    mainTable = quirks.caseFoldTableName("product");
    dependentTable = quirks.caseFoldTableName("sale");

    quirks.cleanupTable(tableName);
    quirks.cleanupTable(mainTable);
    quirks.cleanupTable(dependentTable);
  }

  @AfterEach
  public void afterEach() throws Exception {
    quirks.cleanupTable(tableName);
    quirks.cleanupTable(mainTable);
    quirks.cleanupTable(dependentTable);
    AutoCloseables.close(connection, database, allocator);
  }

  @Test
  public void getInfo() throws Exception {
    try (final ArrowReader reader = connection.getInfo()) {
      assertThat(reader.getVectorSchemaRoot().getSchema())
          .isEqualTo(StandardSchemas.GET_INFO_SCHEMA);
      assertThat(reader.loadNextBatch()).isTrue();
      assertThat(reader.getVectorSchemaRoot().getRowCount()).isGreaterThan(0);
    }
  }

  @Test
  public void getInfoByCode() throws Exception {
    try (final ArrowReader reader =
        connection.getInfo(new AdbcInfoCode[] {AdbcInfoCode.DRIVER_NAME})) {
      final VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertThat(root.getSchema()).isEqualTo(StandardSchemas.GET_INFO_SCHEMA);
      assertThat(reader.loadNextBatch()).isTrue();
      assertThat(root.getRowCount()).isEqualTo(1);
      assertThat(((UInt4Vector) root.getVector(0)).getObject(0))
          .isEqualTo(AdbcInfoCode.DRIVER_NAME.getValue());
      assertThat(
              ((DenseUnionVector) root.getVector(1))
                  .getVarCharVector((byte) 0)
                  .getObject(0)
                  .toString())
          .isNotEmpty();
    }
  }

  @Test
  public void getObjectsConstraints() throws Exception {
    final Schema schema = util.ingestTableWithConstraints(allocator, connection, tableName);
    util.ingestTablesWithReferentialConstraint(allocator, connection, mainTable, dependentTable);

    boolean tableFound = false;
    try (final ArrowReader reader =
        connection.getObjects(AdbcConnection.GetObjectsDepth.ALL, null, null, null, null, null)) {
      assertThat(reader.getVectorSchemaRoot().getSchema())
          .isEqualTo(StandardSchemas.GET_OBJECTS_SCHEMA);
      assertThat(reader.loadNextBatch()).isTrue();

      final ListVector dbSchemas = (ListVector) reader.getVectorSchemaRoot().getVector(1);
      final ListVector dbSchemaTables =
          (ListVector) ((StructVector) dbSchemas.getDataVector()).getVectorById(1);
      final StructVector tables = (StructVector) dbSchemaTables.getDataVector();
      final VarCharVector tableNames = (VarCharVector) tables.getVectorById(0);
      final ListVector tableConstraints = (ListVector) tables.getVectorById(3);

      for (int i = 0; i < tables.getValueCount(); i++) {
        if (tables.isNull(i)) {
          continue;
        }
        final Text tableName = tableNames.getObject(i);
        if (tableName != null && tableName.toString().equalsIgnoreCase(this.tableName)) {
          tableFound = true;

          @SuppressWarnings("unchecked")
          final List<Map<String, ?>> constraints =
              (List<Map<String, ?>>) tableConstraints.getObject(i);

          assertThat(constraints)
              .filteredOn(c -> c.get("constraint_type").equals(new Text("PRIMARY KEY")))
              .extracting("constraint_name")
              .containsExactlyInAnyOrderElementsOf(
                  Collections.singletonList(new Text(quirks.caseFoldColumnName("table_pk"))));

          assertThat(constraints)
              .filteredOn(c -> c.get("constraint_type").equals(new Text("PRIMARY KEY")))
              .flatExtracting("constraint_column_names")
              .containsExactlyInAnyOrderElementsOf(
                  schema.getFields().stream()
                      .map(field -> new Text(field.getName()))
                      .collect(Collectors.toList()));

          assertThat(constraints)
              .filteredOn(c -> c.get("constraint_type").equals(new Text("UNIQUE")))
              .extracting("constraint_name")
              .hasSize(1);

          assertThat(constraints)
              .filteredOn(c -> c.get("constraint_type").equals(new Text("UNIQUE")))
              .flatExtracting("constraint_column_names")
              .containsExactlyInAnyOrderElementsOf(
                  schema.getFields().stream()
                      .map(field -> new Text(field.getName()))
                      .collect(Collectors.toList()));
        }

        if (tableName != null && tableName.toString().equalsIgnoreCase(dependentTable)) {
          @SuppressWarnings("unchecked")
          final List<Map<String, ?>> constraints =
              (List<Map<String, ?>>) tableConstraints.getObject(i);

          assertThat(constraints)
              .extracting("constraint_name")
              .containsExactlyInAnyOrderElementsOf(
                  Collections.singletonList(
                      new Text(quirks.caseFoldColumnName("SALE_PRODUCT_FK"))));

          assertThat(constraints)
              .flatExtracting("constraint_column_names")
              .containsExactlyInAnyOrderElementsOf(
                  Collections.singletonList(new Text(quirks.caseFoldColumnName("product_id"))));

          assertThat(constraints)
              .flatExtracting("constraint_column_usage")
              .asList()
              .first()
              .extracting("fk_table")
              .isEqualTo(new Text(quirks.caseFoldColumnName("product")));
        }
      }
      assertThat(tableFound).describedAs("Table FOO exists in metadata").isTrue();
    }
  }

  @Test
  public void getObjectsColumns() throws Exception {
    final Schema schema = util.ingestTableIntsStrs(allocator, connection, tableName);
    boolean tableFound = false;
    try (final ArrowReader reader =
        connection.getObjects(AdbcConnection.GetObjectsDepth.ALL, null, null, null, null, null)) {
      assertThat(reader.getVectorSchemaRoot().getSchema())
          .isEqualTo(StandardSchemas.GET_OBJECTS_SCHEMA);
      assertThat(reader.loadNextBatch()).isTrue();

      final ListVector dbSchemas = (ListVector) reader.getVectorSchemaRoot().getVector(1);
      final ListVector dbSchemaTables =
          (ListVector) ((StructVector) dbSchemas.getDataVector()).getVectorById(1);
      final StructVector tables = (StructVector) dbSchemaTables.getDataVector();
      final VarCharVector tableNames = (VarCharVector) tables.getVectorById(0);
      final ListVector tableColumns = (ListVector) tables.getVectorById(2);

      for (int i = 0; i < tables.getValueCount(); i++) {
        if (tables.isNull(i)) {
          continue;
        }
        final Text tableName = tableNames.getObject(i);
        if (tableName != null && tableName.toString().equalsIgnoreCase(this.tableName)) {
          tableFound = true;
          @SuppressWarnings("unchecked")
          final List<Map<String, ?>> columns = (List<Map<String, ?>>) tableColumns.getObject(i);
          assertThat(columns)
              .extracting("column_name")
              .containsExactlyInAnyOrderElementsOf(
                  schema.getFields().stream()
                      .map(field -> new Text(field.getName()))
                      .collect(Collectors.toList()));
          assertThat(columns).extracting("ordinal_position").containsExactlyInAnyOrder(1, 2);
        }
      }
    }
    assertThat(tableFound).describedAs("Table FOO exists in metadata").isTrue();
  }

  @Test
  public void getObjectsCatalogs() throws Exception {
    util.ingestTableIntsStrs(allocator, connection, tableName);
    try (final ArrowReader reader =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.CATALOGS, null, null, null, null, null)) {
      assertThat(reader.getVectorSchemaRoot().getSchema())
          .isEqualTo(StandardSchemas.GET_OBJECTS_SCHEMA);
      assertThat(reader.loadNextBatch()).isTrue();
      assertThat(reader.getVectorSchemaRoot().getRowCount()).isGreaterThan(0);
      final FieldVector dbSchemas = reader.getVectorSchemaRoot().getVector(1);
      // We requested depth == CATALOGS, so the db_schemas field should be all null
      assertThat(dbSchemas.getNullCount()).isEqualTo(dbSchemas.getValueCount());
    }
  }

  @Test
  public void getObjectsCatalogsPattern() throws Exception {
    util.ingestTableIntsStrs(allocator, connection, tableName);
    try (final ArrowReader reader =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.CATALOGS,
            quirks.defaultCatalog(),
            null,
            null,
            null,
            null)) {
      assertThat(reader.getVectorSchemaRoot().getSchema())
          .isEqualTo(StandardSchemas.GET_OBJECTS_SCHEMA);
      assertThat(reader.loadNextBatch()).isTrue();
      assertThat(reader.getVectorSchemaRoot().getRowCount()).isEqualTo(1);
      final FieldVector dbSchemas = reader.getVectorSchemaRoot().getVector(1);
      // We requested depth == CATALOGS, so the db_schemas field should be all null
      assertThat(dbSchemas.getNullCount()).isEqualTo(dbSchemas.getValueCount());
      assertThat(reader.loadNextBatch()).isFalse();
    }
  }

  @Test
  public void getObjectsDbSchemas() throws Exception {
    util.ingestTableIntsStrs(allocator, connection, tableName);
    try (final ArrowReader reader =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.DB_SCHEMAS, null, null, null, null, null)) {
      assertThat(reader.getVectorSchemaRoot().getSchema())
          .isEqualTo(StandardSchemas.GET_OBJECTS_SCHEMA);
      assertThat(reader.loadNextBatch()).isTrue();
      assertThat(reader.getVectorSchemaRoot().getRowCount()).isGreaterThan(0);
    }
  }

  @Test
  public void getObjectsTables() throws Exception {
    util.ingestTableIntsStrs(allocator, connection, tableName);
    try (final ArrowReader reader =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.TABLES, null, null, null, null, null)) {
      assertThat(reader.getVectorSchemaRoot().getSchema())
          .isEqualTo(StandardSchemas.GET_OBJECTS_SCHEMA);
      assertThat(reader.loadNextBatch()).isTrue();

      final ListVector dbSchemas = (ListVector) reader.getVectorSchemaRoot().getVector(1);
      final ListVector dbSchemaTables =
          (ListVector) ((StructVector) dbSchemas.getDataVector()).getVectorById(1);
      final StructVector tables = (StructVector) dbSchemaTables.getDataVector();
      final VarCharVector tableNames = (VarCharVector) tables.getVectorById(0);
      assertThat(IntStream.range(0, tableNames.getValueCount()).mapToObj(tableNames::getObject))
          .containsAnyOf(new Text(quirks.caseFoldTableName(tableName)));
    }
  }

  @Test
  public void getTableSchema() throws Exception {
    final Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable(quirks.caseFoldColumnName("INTS"), new ArrowType.Int(32, true)),
                Field.nullable(quirks.caseFoldColumnName("STRS"), new ArrowType.Utf8())));
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.CREATE)) {
      stmt.bind(root);
      stmt.executeUpdate();
    }
    assertThat(connection.getTableSchema(/*catalog*/ null, /*dbSchema*/ null, tableName))
        .isEqualTo(schema);
  }

  @Test
  public void getTableSchemaDoesNotExist() throws Exception {
    final AdbcException thrown =
        assertThrows(
            AdbcException.class,
            () -> connection.getTableSchema(/*catalog*/ null, /*dbSchema*/ null, "DOESNOTEXIST"));
    assertThat(thrown.getStatus())
        .describedAs(thrown.toString())
        .isEqualTo(AdbcStatusCode.NOT_FOUND);
  }

  @Test
  public void getTableTypes() throws Exception {
    try (final ArrowReader reader = connection.getTableTypes()) {
      assertThat(reader.getVectorSchemaRoot().getSchema())
          .isEqualTo(StandardSchemas.TABLE_TYPES_SCHEMA);
      List<String> tableTypes = new ArrayList<>();
      while (reader.loadNextBatch()) {
        final VarCharVector types = (VarCharVector) reader.getVectorSchemaRoot().getVector(0);
        for (int i = 0; i < types.getValueCount(); i++) {
          assertThat(types.isNull(i)).isFalse();
          tableTypes.add(types.getObject(i).toString());
        }
      }
      assertThat(tableTypes).anyMatch("table"::equalsIgnoreCase);
    }
  }
}
