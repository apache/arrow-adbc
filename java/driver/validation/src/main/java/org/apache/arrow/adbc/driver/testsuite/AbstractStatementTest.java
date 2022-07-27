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

import static org.apache.arrow.adbc.driver.testsuite.ArrowAssertions.assertRoot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class AbstractStatementTest {
  /** Must be initialized by the subclass. */
  protected static SqlValidationQuirks quirks;

  protected AdbcDatabase database;
  protected AdbcConnection connection;
  protected BufferAllocator allocator;
  protected SqlTestUtil util;
  protected String tableName;
  protected Schema schema;

  @BeforeEach
  public void beforeEach() throws Exception {
    Preconditions.checkNotNull(quirks, "Must initialize quirks in subclass with @BeforeAll");
    database = quirks.initDatabase();
    connection = database.connect();
    allocator = new RootAllocator();
    util = new SqlTestUtil(quirks);
    tableName = quirks.caseFoldTableName("bulktable");
    schema =
        new Schema(
            Arrays.asList(
                Field.nullable(
                    quirks.caseFoldColumnName("ints"), new ArrowType.Int(32, /*signed=*/ true)),
                Field.nullable(quirks.caseFoldColumnName("strs"), new ArrowType.Utf8())));
    quirks.cleanupTable(tableName);
  }

  @AfterEach
  public void afterEach() throws Exception {
    quirks.cleanupTable(tableName);
    AutoCloseables.close(connection, database, allocator);
  }

  @Test
  public void bulkIngestAppend() throws Exception {
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

      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.CREATE)) {
        stmt.bind(root);
        stmt.execute();
      }
      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery("SELECT * FROM " + tableName);
        try (ArrowReader arrowReader = stmt.executeQuery()) {
          assertThat(arrowReader.loadNextBatch()).isTrue();
          assertRoot(arrowReader.getVectorSchemaRoot()).isEqualTo(root);
        }
      }

      // Append
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.APPEND)) {
        stmt.bind(root);
        stmt.execute();
      }
      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery("SELECT * FROM " + tableName);
        try (ArrowReader arrowReader = stmt.executeQuery()) {
          assertThat(arrowReader.loadNextBatch()).isTrue();
          root.setRowCount(8);
          ints.setSafe(4, 0);
          ints.setSafe(5, 1);
          ints.setSafe(6, 2);
          ints.setNull(7);
          strs.setNull(4);
          strs.setSafe(5, "foo".getBytes(StandardCharsets.UTF_8));
          strs.setSafe(6, "".getBytes(StandardCharsets.UTF_8));
          strs.setSafe(7, "asdf".getBytes(StandardCharsets.UTF_8));
          assertRoot(arrowReader.getVectorSchemaRoot()).isEqualTo(root);
        }
      }
    }
  }

  @Test
  public void bulkIngestAppendConflict() throws Exception {
    final Schema schema2 =
        new Schema(
            Collections.singletonList(
                Field.nullable(quirks.caseFoldColumnName("ints"), new ArrowType.Utf8())));
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.setRowCount(1);
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.CREATE)) {
        stmt.bind(root);
        stmt.execute();
      }
    }
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema2, allocator)) {
      root.setRowCount(1);
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.APPEND)) {
        stmt.bind(root);
        assertThrows(AdbcException.class, stmt::execute);
      }
    }
  }

  @Test
  public void bulkIngestAppendNotFound() throws Exception {
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.setRowCount(1);
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.APPEND)) {
        stmt.bind(root);
        final AdbcException e = assertThrows(AdbcException.class, stmt::execute);
        assertThat(e.getStatus()).describedAs("%s", e).isEqualTo(AdbcStatusCode.NOT_FOUND);
      }
    }
  }

  @Test
  public void bulkIngestCreateConflict() throws Exception {
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.setRowCount(1);
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.CREATE)) {
        stmt.bind(root);
        stmt.execute();
      }
    }
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.CREATE)) {
        stmt.bind(root);
        final AdbcException e = assertThrows(AdbcException.class, stmt::execute);
        assertThat(e.getStatus()).describedAs("%s", e).isEqualTo(AdbcStatusCode.ALREADY_EXISTS);
      }
    }
  }

  @Test
  public void prepareQuery() throws Exception {
    final Schema expectedSchema = util.ingestTableIntsStrs(allocator, connection, tableName);
    try (final AdbcStatement stmt = connection.createStatement()) {
      stmt.setSqlQuery("SELECT * FROM " + tableName);
      stmt.prepare();
      try (final ArrowReader reader = stmt.executeQuery()) {
        assertThat(reader.getVectorSchemaRoot().getSchema()).isEqualTo(expectedSchema);
        assertThat(reader.loadNextBatch()).isTrue();
        assertThat(reader.getVectorSchemaRoot().getRowCount()).isEqualTo(4);
        while (reader.loadNextBatch()) {
          assertThat(reader.getVectorSchemaRoot().getRowCount()).isEqualTo(0);
        }
      }
    }
  }

  @Test
  public void prepareQueryWithParameters() throws Exception {
    final Schema expectedSchema = util.ingestTableIntsStrs(allocator, connection, tableName);
    final Schema paramsSchema =
        new Schema(Collections.singletonList(expectedSchema.getFields().get(0)));
    try (final AdbcStatement stmt = connection.createStatement();
        final VectorSchemaRoot params = VectorSchemaRoot.create(paramsSchema, allocator)) {
      stmt.setSqlQuery(String.format("SELECT * FROM %s WHERE INTS = ?", tableName));
      stmt.prepare();
      stmt.bind(params);
      IntVector param0 = (IntVector) params.getVector(0);
      param0.setSafe(0, 1);
      param0.setSafe(1, 2);
      params.setRowCount(2);
      try (final ArrowReader reader = stmt.executeQuery()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        assertThat(root.getSchema()).isEqualTo(expectedSchema);
        assertThat(reader.loadNextBatch()).isTrue();
        assertThat(root.getRowCount()).isEqualTo(1);
        assertThat(root.getVector(1).getObject(0)).isEqualTo(new Text("foo"));

        assertThat(reader.loadNextBatch()).isTrue();
        assertThat(root.getRowCount()).isEqualTo(1);
        assertThat(root.getVector(1).getObject(0)).isEqualTo(new Text(""));

        assertThat(reader.loadNextBatch()).isFalse();
      }

      param0.setSafe(0, 0);
      params.setRowCount(1);
      try (final ArrowReader reader = stmt.executeQuery()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        assertThat(root.getSchema()).isEqualTo(expectedSchema);
        assertThat(reader.loadNextBatch()).isTrue();
        assertThat(root.getRowCount()).isEqualTo(1);
        assertThat(root.getVector(1).getObject(0)).isNull();

        assertThat(reader.loadNextBatch()).isFalse();
      }
    }
  }
}
