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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.core.PartitionDescriptor;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class AbstractPartitionDescriptorTest {
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
  public void serializeDeserializeQuery() throws Exception {
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
      final List<PartitionDescriptor> descriptors;
      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery("SELECT * FROM " + tableName);
        stmt.execute();
        descriptors = stmt.getPartitionDescriptors();
        // For convenience, assume database won't shard 4 rows over more than 1 partition…
        assertThat(descriptors).hasSize(1);
      }

      // The serialized partition descriptor should be executable on a separate connection
      try (final AdbcConnection connection2 = database.connect();
          final AdbcStatement stmt =
              connection2.deserializePartitionDescriptor(descriptors.get(0).getDescriptor());
          final ArrowReader reader = stmt.getArrowReader()) {
        assertThat(reader.loadNextBatch()).isTrue();
        assertThat(reader.getVectorSchemaRoot().getSchema()).isEqualTo(root.getSchema());
        assertRoot(reader.getVectorSchemaRoot()).isEqualTo(root);
      }
    }
  }
}
