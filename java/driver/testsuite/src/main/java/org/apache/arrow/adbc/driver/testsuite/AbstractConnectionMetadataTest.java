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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.StandardSchemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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

public abstract class AbstractConnectionMetadataTest {
  protected AdbcDatabase database;
  protected AdbcConnection connection;
  protected BufferAllocator allocator;

  protected abstract AdbcDatabase init() throws AdbcException;

  @BeforeEach
  public void beforeEach() throws Exception {
    database = init();
    connection = database.connect();
    allocator = new RootAllocator();
  }

  @AfterEach
  public void afterEach() throws Exception {
    AutoCloseables.close(connection, database, allocator);
  }

  @Test
  void getObjectsColumns() throws Exception {
    loadTable();
    boolean tableFound = false;
    try (final AdbcStatement stmt =
        connection.getObjects(AdbcConnection.GetObjectsDepth.ALL, null, null, null, null, null)) {
      try (final ArrowReader reader = stmt.getArrowReader()) {
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
          if (tableName != null && tableName.toString().equals("FOO")) {
            tableFound = true;
            @SuppressWarnings("unchecked")
            final List<Map<String, ?>> columns = (List<Map<String, ?>>) tableColumns.getObject(i);
            assertThat(columns)
                .extracting("column_name")
                .containsExactlyInAnyOrder(new Text("INTS"), new Text("STRS"));
            assertThat(columns).extracting("ordinal_position").containsExactlyInAnyOrder(1, 2);
          }
        }
      }
    }
    assertThat(tableFound).describedAs("Table FOO exists in metadata").isTrue();
  }

  @Test
  void getObjectsCatalogs() throws Exception {
    loadTable();
    try (final AdbcStatement stmt =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.CATALOGS, null, null, null, null, null)) {
      try (final ArrowReader reader = stmt.getArrowReader()) {
        assertThat(reader.getVectorSchemaRoot().getSchema())
            .isEqualTo(StandardSchemas.GET_OBJECTS_SCHEMA);
        assertThat(reader.loadNextBatch()).isTrue();
        assertThat(reader.getVectorSchemaRoot().getRowCount()).isGreaterThan(0);
        final FieldVector dbSchemas = reader.getVectorSchemaRoot().getVector(1);
        // We requested depth == CATALOGS, so the db_schemas field should be all null
        assertThat(dbSchemas.getNullCount()).isEqualTo(dbSchemas.getValueCount());
      }
    }
  }

  @Test
  void getObjectsDbSchemas() throws Exception {
    loadTable();
    try (final AdbcStatement stmt =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.DB_SCHEMAS, null, null, null, null, null)) {
      try (final ArrowReader reader = stmt.getArrowReader()) {
        assertThat(reader.getVectorSchemaRoot().getSchema())
            .isEqualTo(StandardSchemas.GET_OBJECTS_SCHEMA);
        assertThat(reader.loadNextBatch()).isTrue();
        assertThat(reader.getVectorSchemaRoot().getRowCount()).isGreaterThan(0);
      }
    }
  }

  @Test
  void getObjectsTables() throws Exception {
    loadTable();
    try (final AdbcStatement stmt =
        connection.getObjects(
            AdbcConnection.GetObjectsDepth.TABLES, null, null, null, null, null)) {
      try (final ArrowReader reader = stmt.getArrowReader()) {
        assertThat(reader.getVectorSchemaRoot().getSchema())
            .isEqualTo(StandardSchemas.GET_OBJECTS_SCHEMA);
        assertThat(reader.loadNextBatch()).isTrue();

        final ListVector dbSchemas = (ListVector) reader.getVectorSchemaRoot().getVector(1);
        final ListVector dbSchemaTables =
            (ListVector) ((StructVector) dbSchemas.getDataVector()).getVectorById(1);
        final StructVector tables = (StructVector) dbSchemaTables.getDataVector();
        final VarCharVector tableNames = (VarCharVector) tables.getVectorById(0);
        assertThat(IntStream.range(0, tableNames.getValueCount()).mapToObj(tableNames::getObject))
            .contains(new Text("FOO"));
      }
    }
  }

  @Test
  void getTableSchema() throws Exception {
    final Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("INTS", new ArrowType.Int(32, /*signed=*/ true)),
                Field.nullable("STRS", new ArrowType.Utf8())));
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      try (final AdbcStatement stmt = connection.bulkIngest("FOO")) {
        stmt.bind(root);
        stmt.execute();
      }
    }
    assertThat(connection.getTableSchema(/*catalog*/ null, /*dbSchema*/ null, "FOO"))
        .isEqualTo(schema);
  }

  @Test
  void getTableTypes() throws Exception {
    try (final AdbcStatement stmt = connection.getTableTypes()) {
      try (final ArrowReader reader = stmt.getArrowReader()) {
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

  void loadTable() throws Exception {
    final Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("INTS", new ArrowType.Int(32, /*signed=*/ true)),
                Field.nullable("STRS", new ArrowType.Utf8())));
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final IntVector ints = (IntVector) root.getVector(0);
      final VarCharVector strs = (VarCharVector) root.getVector(1);

      ints.allocateNew(4);
      ints.setSafe(0, 0);
      ints.setSafe(1, 1);
      ints.setSafe(2, 2);
      ints.setNull(3);
      strs.allocateNew(4);
      strs.setNull(0);
      strs.setSafe(1, "foo".getBytes(StandardCharsets.UTF_8));
      strs.setSafe(2, "".getBytes(StandardCharsets.UTF_8));
      strs.setSafe(3, "asdf".getBytes(StandardCharsets.UTF_8));
      root.setRowCount(4);

      // TODO: XXX: need a "quirks" system to handle idiosyncracies. For example: Derby forces table
      // names to uppercase, but does not do case folding in all places.
      try (final AdbcStatement stmt = connection.bulkIngest("FOO")) {
        stmt.bind(root);
        stmt.execute();
      }
    }
  }
}
