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

package org.apache.arrow.adbc.driver.derby;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DerbyDatabaseTest {
  @Test
  void simpleQuery(@TempDir Path tempDir) throws Exception {
    final Map<String, String> parameters = new HashMap<>();
    parameters.put("path", tempDir.toString() + "/db");
    try (final AdbcDatabase database = DerbyDriver.INSTANCE.connect(parameters);
        final AdbcConnection connection = database.connect();
        final AdbcStatement statement = connection.createStatement()) {
      statement.setSqlQuery("SELECT * FROM SYS.SYSTABLES");
      statement.execute();
      try (final ArrowReader reader = statement.getArrowReader()) {
        assertThat(reader.loadNextBatch()).isTrue();
      }
    }
  }

  @Test
  void bulkInsert(@TempDir Path tempDir) throws Exception {
    final Map<String, String> parameters = new HashMap<>();
    parameters.put("path", tempDir.toString() + "/db");
    final Schema schema =
        new Schema(
            Collections.singletonList(
                Field.nullable("ints", new ArrowType.Int(32, /*signed=*/ true))));
    try (final BufferAllocator allocator = new RootAllocator();
        final AdbcDatabase database = DerbyDriver.INSTANCE.connect(parameters);
        final AdbcConnection connection = database.connect()) {
      try (final AdbcStatement statement = connection.createStatement();
          final VectorSchemaRoot ingest = VectorSchemaRoot.create(schema, allocator)) {
        statement.setOption(AdbcStatement.INGEST_OPTION_TARGET_TABLE, "foo");
        statement.bind(ingest);
        final IntVector ints = (IntVector) ingest.getVector(0);
        ints.allocateNew(4);
        ints.setSafe(0, 0);
        ints.setSafe(1, 1);
        ints.setSafe(2, 2);
        ints.setSafe(3, 3);
        ingest.setRowCount(4);
        statement.execute();
      }

      try (final AdbcStatement statement = connection.createStatement()) {
        statement.setSqlQuery("SELECT * FROM foo");
        statement.execute();

        try (final ArrowReader reader = statement.getArrowReader()) {
          assertThat(reader.loadNextBatch()).isTrue();
          final FieldVector vector = reader.getVectorSchemaRoot().getVector(0);
          assertThat(vector).isInstanceOf(IntVector.class);
          assertThat(vector.getValueCount()).isEqualTo(4);
          final IntVector ints = (IntVector) vector;
          for (int i = 0; i < 4; i++) {
            assertThat(ints.isNull(i)).isFalse();
            assertThat(ints.get(i)).isEqualTo(i);
          }
          assertThat(reader.loadNextBatch()).isFalse();
        }
      }
    }
  }
}
