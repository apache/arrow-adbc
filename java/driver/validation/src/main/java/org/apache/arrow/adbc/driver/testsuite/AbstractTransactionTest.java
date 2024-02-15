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

import static org.apache.arrow.adbc.driver.testsuite.ArrowAssertions.assertAdbcException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class AbstractTransactionTest {
  /** Must be initialized by the subclass. */
  protected static SqlValidationQuirks quirks;

  protected BufferAllocator allocator;
  protected AdbcDatabase database;
  protected AdbcConnection connection;

  @BeforeEach
  public void beforeEach() throws Exception {
    Preconditions.checkNotNull(quirks, "Must initialize quirks in subclass with @BeforeAll");
    allocator = new RootAllocator();
    database = quirks.initDatabase(allocator);
    connection = database.connect();
  }

  @AfterEach
  public void afterEach() throws Exception {
    AutoCloseables.close(connection, database, allocator);
  }

  @Test
  void autoCommitByDefault() throws Exception {
    assertAdbcException(assertThrows(AdbcException.class, () -> connection.commit()))
        .isStatus(AdbcStatusCode.INVALID_STATE);
    assertAdbcException(assertThrows(AdbcException.class, () -> connection.rollback()))
        .isStatus(AdbcStatusCode.INVALID_STATE);
    assertThat(connection.getAutoCommit()).isTrue();
  }

  @Test
  void toggleAutoCommit() throws Exception {
    assertThat(connection.getAutoCommit()).isTrue();
    connection.setAutoCommit(true);
    assertThat(connection.getAutoCommit()).isTrue();
    connection.setAutoCommit(false);
    assertThat(connection.getAutoCommit()).isFalse();
    connection.setAutoCommit(true);
    assertThat(connection.getAutoCommit()).isTrue();
  }

  @Test
  void rollback() throws Exception {
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("ints", new ArrowType.Int(32, true))));

    connection.setAutoCommit(false);
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final IntVector ints = (IntVector) root.getVector(0);
      ints.setSafe(0, 1);
      ints.setSafe(1, 2);
      root.setRowCount(2);
      try (final AdbcStatement stmt = connection.bulkIngest("foo", BulkIngestMode.CREATE)) {
        stmt.bind(root);
        stmt.executeUpdate();
      }
      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery("SELECT * FROM foo");
        stmt.executeQuery().close();
      }
      connection.rollback();
      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery("SELECT * FROM foo");
        assertThrows(AdbcException.class, stmt::executeQuery);
      }
    }

    quirks.cleanupTable("foo");
  }

  @Test
  void commit() throws Exception {
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("ints", new ArrowType.Int(32, true))));
    final String tableName = quirks.caseFoldTableName("temptable");

    connection.setAutoCommit(false);
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final IntVector ints = (IntVector) root.getVector(0);
      ints.setSafe(0, 1);
      ints.setSafe(1, 2);
      root.setRowCount(2);
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.CREATE)) {
        stmt.bind(root);
        stmt.executeUpdate();
      }
      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery("SELECT * FROM " + tableName);
        stmt.executeQuery().close();
      }
      connection.commit();
      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery("SELECT * FROM " + tableName);
        stmt.executeQuery().close();
      }
      connection.commit();
    }

    quirks.cleanupTable(tableName);
  }

  @Test
  void enableAutoCommitAlsoCommits() throws Exception {
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("ints", new ArrowType.Int(32, true))));
    final String tableName = quirks.caseFoldTableName("temptable");

    connection.setAutoCommit(false);
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final IntVector ints = (IntVector) root.getVector(0);
      ints.setSafe(0, 1);
      ints.setSafe(1, 2);
      root.setRowCount(2);
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.CREATE)) {
        stmt.bind(root);
        stmt.executeUpdate();
      }
      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery("SELECT * FROM " + tableName);
        stmt.executeQuery().close();
      }
      connection.setAutoCommit(true);
      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery("SELECT * FROM " + tableName);
        stmt.executeQuery().close();
      }
    }

    quirks.cleanupTable(tableName);
  }
}
