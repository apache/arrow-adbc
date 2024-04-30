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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public final class SqlTestUtil {
  private final SqlValidationQuirks quirks;

  public SqlTestUtil(SqlValidationQuirks quirks) {
    this.quirks = quirks;
  }

  /** Check if we are running in the Arrow CI. */
  public static boolean isCI() {
    // Set by GitHub Actions
    return "true".equals(System.getenv("CI"));
  }

  /** Load a simple table with two columns. */
  public Schema ingestTableIntsStrs(
      BufferAllocator allocator, AdbcConnection connection, String tableName) throws Exception {
    tableName = quirks.caseFoldTableName(tableName);
    final Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable(quirks.caseFoldColumnName("INTS"), Types.MinorType.INT.getType()),
                Field.nullable(
                    quirks.caseFoldColumnName("STRS"), Types.MinorType.VARCHAR.getType())));
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
        stmt.executeUpdate();
      }
    }
    return schema;
  }

  /** Load a table with composite primary key */
  public Schema ingestTableWithConstraints(
      BufferAllocator allocator, AdbcConnection connection, String tableName) throws Exception {
    tableName = quirks.caseFoldTableName(tableName);
    final Schema schema =
        new Schema(
            Arrays.asList(
                Field.notNullable(quirks.caseFoldColumnName("INTS"), new ArrowType.Int(32, true)),
                Field.nullable(quirks.caseFoldColumnName("INTS2"), new ArrowType.Int(32, true))));
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final IntVector ints = (IntVector) root.getVector(0);
      final IntVector strs = (IntVector) root.getVector(1);

      ints.allocateNew(4);
      ints.setSafe(0, 0);
      ints.setSafe(1, 1);
      ints.setSafe(2, 2);
      ints.setSafe(3, 3);
      strs.allocateNew(4);
      strs.setSafe(0, 10);
      strs.setSafe(1, 11);
      strs.setSafe(2, 12);
      strs.setSafe(3, 13);
      root.setRowCount(4);
      try (final AdbcStatement stmt = connection.bulkIngest(tableName, BulkIngestMode.CREATE)) {
        stmt.bind(root);
        stmt.executeUpdate();
      }

      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery(quirks.generateSetNotNullQuery(tableName, "INTS"));
        stmt.executeUpdate();
      }

      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery(quirks.generateSetNotNullQuery(tableName, "INTS2"));
        stmt.executeUpdate();
      }

      try (final AdbcStatement stmt = connection.createStatement()) {
        stmt.setSqlQuery(
            quirks.generateAddPrimaryKeyQuery(
                "TABLE_PK", tableName, Arrays.asList("INTS", "INTS2")));
        stmt.executeUpdate();
      }
    }
    return schema;
  }

  /** Load two tables with foreign key relationship between them */
  public void ingestTablesWithReferentialConstraint(
      BufferAllocator allocator, AdbcConnection connection, String mainTable, String dependentTable)
      throws Exception {
    mainTable = quirks.caseFoldTableName(mainTable);
    dependentTable = quirks.caseFoldTableName(dependentTable);

    final Schema mainSchema =
        new Schema(
            Collections.singletonList(
                Field.notNullable(
                    quirks.caseFoldColumnName("PRODUCT_ID"), new ArrowType.Int(32, true))));

    final Schema dependentSchema =
        new Schema(
            Arrays.asList(
                Field.notNullable(
                    quirks.caseFoldColumnName("SALE_ID"), new ArrowType.Int(32, true)),
                Field.notNullable(
                    quirks.caseFoldColumnName("PRODUCT_ID"), new ArrowType.Int(32, true))));

    try (final VectorSchemaRoot root = VectorSchemaRoot.create(mainSchema, allocator)) {
      final IntVector product = (IntVector) root.getVector(0);
      product.allocateNew(4);
      product.setSafe(0, 1);
      product.setSafe(1, 2);
      product.setSafe(2, 3);
      product.setSafe(3, 4);
      root.setRowCount(4);
      try (final AdbcStatement stmt = connection.bulkIngest(mainTable, BulkIngestMode.CREATE)) {
        stmt.bind(root);
        stmt.executeUpdate();
      }
    }

    try (final VectorSchemaRoot root = VectorSchemaRoot.create(dependentSchema, allocator)) {
      final IntVector sale = (IntVector) root.getVector(0);
      final IntVector product = (IntVector) root.getVector(1);

      sale.allocateNew(2);
      sale.setSafe(0, 1);
      sale.setSafe(1, 2);
      product.allocateNew(2);
      product.setSafe(0, 2);
      product.setSafe(1, 4);
      root.setRowCount(2);
      try (final AdbcStatement stmt =
          connection.bulkIngest(dependentTable, BulkIngestMode.CREATE)) {
        stmt.bind(root);
        stmt.executeUpdate();
      }
    }

    try (final AdbcStatement stmt = connection.createStatement()) {
      stmt.setSqlQuery(quirks.generateSetNotNullQuery(mainTable, "PRODUCT_ID"));
      stmt.executeUpdate();
    }

    try (final AdbcStatement stmt = connection.createStatement()) {
      stmt.setSqlQuery(
          quirks.generateAddPrimaryKeyQuery(
              "PRODUCT_PK", mainTable, Collections.singletonList("PRODUCT_ID")));
      stmt.executeUpdate();
    }

    try (final AdbcStatement stmt = connection.createStatement()) {
      stmt.setSqlQuery(
          quirks.generateAddForeignKeyQuery(
              "SALE_PRODUCT_FK", dependentTable, "PRODUCT_ID", mainTable, "PRODUCT_ID"));
      stmt.executeUpdate();
    }
  }
}
