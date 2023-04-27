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

import static org.apache.arrow.adbc.driver.testsuite.ArrowAssertions.assertField;
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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
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
  // Implementations vary on the integer type
  protected Schema schema32;
  protected Schema schema64;

  @BeforeEach
  public void beforeEach() throws Exception {
    Preconditions.checkNotNull(quirks, "Must initialize quirks in subclass with @BeforeAll");
    allocator = new RootAllocator();
    database = quirks.initDatabase(allocator);
    connection = database.connect();
    util = new SqlTestUtil(quirks);
    tableName = quirks.caseFoldTableName("bulktable");
    schema32 =
        new Schema(
            Arrays.asList(
                Field.nullable(quirks.caseFoldColumnName("ints"), Types.MinorType.INT.getType()),
                Field.nullable(
                    quirks.caseFoldColumnName("strs"), Types.MinorType.VARCHAR.getType())));
    schema64 =
        new Schema(
            Arrays.asList(
                Field.nullable(quirks.caseFoldColumnName("ints"), Types.MinorType.BIGINT.getType()),
                Field.nullable(
                    quirks.caseFoldColumnName("strs"), Types.MinorType.VARCHAR.getType())));
    quirks.cleanupTable(tableName);
  }

  @AfterEach
  public void afterEach() throws Exception {
    quirks.cleanupTable(tableName);
    AutoCloseables.close(connection, database, allocator);
  }

  @Test
  public void bulkIngestAppend() throws Exception {
    // Implementations vary on the integer type here.
    try (final VectorSchemaRoot root32 = VectorSchemaRoot.create(schema32, allocator);
        final VectorSchemaRoot root64 = VectorSchemaRoot.create(schema64, allocator)) {
      {
        final IntVector ints = (IntVector) root32.getVector(0);
        final VarCharVector strs = (VarCharVector) root32.getVector(1);

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
        root32.setRowCount(4);
      }
      {
        final BigIntVector ints = (BigIntVector) root64.getVector(0);
        final VarCharVector strs = (VarCharVector) root64.getVector(1);

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
        root64.setRowCount(4);
      }

      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.CREATE)) {
        stmt.bind(root32);
        stmt.executeUpdate();
      }
      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery("SELECT * FROM " + tableName);
        try (AdbcStatement.QueryResult queryResult = stmt.executeQuery()) {
          assertThat(queryResult.getReader().loadNextBatch()).isTrue();
          assertThat(queryResult.getReader().getVectorSchemaRoot())
              .satisfiesAnyOf(
                  data -> assertRoot(data).isEqualTo(root32),
                  data -> assertRoot(data).isEqualTo(root64));
        }
      }

      // Append
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.APPEND)) {
        stmt.bind(root32);
        stmt.executeUpdate();
      }
      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery("SELECT * FROM " + tableName);
        try (AdbcStatement.QueryResult queryResult = stmt.executeQuery()) {
          assertThat(queryResult.getReader().loadNextBatch()).isTrue();
          {
            root32.setRowCount(8);
            final IntVector ints = (IntVector) root32.getVector(0);
            final VarCharVector strs = (VarCharVector) root32.getVector(1);
            ints.setSafe(4, 0);
            ints.setSafe(5, 1);
            ints.setSafe(6, 2);
            ints.setNull(7);
            strs.setNull(4);
            strs.setSafe(5, "foo".getBytes(StandardCharsets.UTF_8));
            strs.setSafe(6, "".getBytes(StandardCharsets.UTF_8));
            strs.setSafe(7, "asdf".getBytes(StandardCharsets.UTF_8));
          }
          {
            root64.setRowCount(8);
            final BigIntVector ints = (BigIntVector) root64.getVector(0);
            final VarCharVector strs = (VarCharVector) root64.getVector(1);
            ints.setSafe(4, 0);
            ints.setSafe(5, 1);
            ints.setSafe(6, 2);
            ints.setNull(7);
            strs.setNull(4);
            strs.setSafe(5, "foo".getBytes(StandardCharsets.UTF_8));
            strs.setSafe(6, "".getBytes(StandardCharsets.UTF_8));
            strs.setSafe(7, "asdf".getBytes(StandardCharsets.UTF_8));
          }
          assertThat(queryResult.getReader().getVectorSchemaRoot())
              .satisfiesAnyOf(
                  data -> assertRoot(data).isEqualTo(root32),
                  data -> assertRoot(data).isEqualTo(root64));
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
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema32, allocator)) {
      root.setRowCount(1);
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.CREATE)) {
        stmt.bind(root);
        stmt.executeUpdate();
      }
    }
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema2, allocator)) {
      root.setRowCount(1);
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.APPEND)) {
        stmt.bind(root);
        assertThrows(AdbcException.class, stmt::executeUpdate);
      }
    }
  }

  @Test
  public void bulkIngestAppendNotFound() throws Exception {
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema32, allocator)) {
      root.setRowCount(1);
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.APPEND)) {
        stmt.bind(root);
        final AdbcException e = assertThrows(AdbcException.class, stmt::executeUpdate);
        assertThat(e.getStatus()).describedAs("%s", e).isEqualTo(AdbcStatusCode.NOT_FOUND);
      }
    }
  }

  @Test
  public void bulkIngestCreateConflict() throws Exception {
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema32, allocator)) {
      root.setRowCount(1);
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.CREATE)) {
        stmt.bind(root);
        stmt.executeUpdate();
      }
    }
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema32, allocator)) {
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.CREATE)) {
        stmt.bind(root);
        final AdbcException e = assertThrows(AdbcException.class, stmt::executeUpdate);
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
      try (AdbcStatement.QueryResult queryResult = stmt.executeQuery()) {
        // Implementations vary on the integer type here.
        Schema actualSchema = queryResult.getReader().getVectorSchemaRoot().getSchema();
        assertThat(actualSchema.getFields().size()).isEqualTo(2);
        assertThat(actualSchema.getFields().get(0).getType())
            .isIn(Types.MinorType.INT.getType(), Types.MinorType.BIGINT.getType());
        assertField(actualSchema.getFields().get(1)).isEqualTo(expectedSchema.getFields().get(1));
        assertThat(queryResult.getReader().loadNextBatch()).isTrue();
        assertThat(queryResult.getReader().getVectorSchemaRoot().getRowCount()).isEqualTo(4);
        while (queryResult.getReader().loadNextBatch()) {
          assertThat(queryResult.getReader().getVectorSchemaRoot().getRowCount()).isEqualTo(0);
        }
      }
    }
  }

  @Test
  public void prepareQueryWithParametersNoOp() throws Exception {
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
      // Ensure things still close properly if we never actually drain the result
      stmt.executeQuery().close();
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
      try (AdbcStatement.QueryResult queryResult = stmt.executeQuery()) {
        VectorSchemaRoot root = queryResult.getReader().getVectorSchemaRoot();
        assertThat(root.getSchema()).isEqualTo(expectedSchema);
        assertThat(queryResult.getReader().loadNextBatch()).isTrue();
        assertThat(root.getRowCount()).isEqualTo(1);
        assertThat(root.getVector(1).getObject(0)).isEqualTo(new Text("foo"));

        assertThat(queryResult.getReader().loadNextBatch()).isTrue();
        assertThat(root.getRowCount()).isEqualTo(1);
        assertThat(root.getVector(1).getObject(0)).isEqualTo(new Text(""));

        assertThat(queryResult.getReader().loadNextBatch()).isFalse();
      }

      param0.setSafe(0, 0);
      params.setRowCount(1);
      try (AdbcStatement.QueryResult queryResult = stmt.executeQuery()) {
        VectorSchemaRoot root = queryResult.getReader().getVectorSchemaRoot();
        assertThat(root.getSchema()).isEqualTo(expectedSchema);
        assertThat(queryResult.getReader().loadNextBatch()).isTrue();
        assertThat(root.getRowCount()).isEqualTo(1);
        assertThat(root.getVector(1).getObject(0)).isNull();

        assertThat(queryResult.getReader().loadNextBatch()).isFalse();
      }
    }
  }

  @Test
  public void getParameterSchema() throws Exception {
    util.ingestTableIntsStrs(allocator, connection, tableName);
    try (final AdbcStatement stmt = connection.createStatement()) {
      stmt.setSqlQuery(String.format("SELECT * FROM %s WHERE INTS = ?", tableName));
      stmt.prepare();
      final Schema paramsSchema = stmt.getParameterSchema();
      // Golang SQLite Flight SQL server doesn't support this
      assertThat(paramsSchema).isNotNull();
    }
  }
}
