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
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public final class SqlTestUtil {
  private final SqlValidationQuirks quirks;

  public SqlTestUtil(SqlValidationQuirks quirks) {
    this.quirks = quirks;
  }

  /** Load a simple table with two columns. */
  public Schema ingestTableIntsStrs(
      BufferAllocator allocator, AdbcConnection connection, String tableName) throws Exception {
    tableName = quirks.caseFoldTableName(tableName);
    final Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable(
                    quirks.caseFoldColumnName("INTS"), new ArrowType.Int(32, /*signed=*/ true)),
                Field.nullable(quirks.caseFoldColumnName("STRS"), new ArrowType.Utf8())));
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
}
