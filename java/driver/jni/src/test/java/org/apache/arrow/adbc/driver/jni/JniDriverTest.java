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

package org.apache.arrow.adbc.driver.jni;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

class JniDriverTest {
  @Test
  void minimal() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");

      driver.open(parameters).close();
    }
  }

  @Test
  void querySimple() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");

      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect();
          final AdbcStatement stmt = conn.createStatement()) {
        stmt.setSqlQuery("SELECT 1");
        try (final AdbcStatement.QueryResult result = stmt.executeQuery()) {
          assertThat(result.getReader().loadNextBatch()).isTrue();
          assertThat(result.getReader().getVectorSchemaRoot().getVector(0).getObject(0))
              .isEqualTo(1L);
        }
      }
    }
  }

  @Test
  void queryLarge() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");

      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect();
          final AdbcStatement stmt = conn.createStatement()) {
        stmt.setSqlQuery(
            "WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < 65536)"
                + " SELECT * FROM seq");
        try (final AdbcStatement.QueryResult result = stmt.executeQuery()) {
          List<Long> seen = new ArrayList<>();
          List<Long> expected = LongStream.range(1, 65537).boxed().collect(Collectors.toList());
          while (result.getReader().loadNextBatch()) {
            VectorSchemaRoot vsr = result.getReader().getVectorSchemaRoot();
            //noinspection resource
            BigIntVector i =
                assertThat(vsr.getVector(0))
                    .asInstanceOf(InstanceOfAssertFactories.type(BigIntVector.class))
                    .actual();
            for (int index = 0; index < vsr.getRowCount(); index++) {
              assertThat(i.isNull(index)).isFalse();
              seen.add(i.get(index));
            }
          }
          assertThat(seen).isEqualTo(expected);
        }
      }
    }
  }

  @Test
  void queryError() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");

      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect();
          final AdbcStatement stmt = conn.createStatement()) {
        stmt.setSqlQuery("SELECT ?");
        AdbcException exc =
            assertThrows(
                AdbcException.class,
                () -> {
                  //noinspection EmptyTryBlock
                  try (final AdbcStatement.QueryResult result = stmt.executeQuery()) {}
                });
        assertThat(exc.getStatus()).isEqualTo(AdbcStatusCode.INVALID_STATE);
        assertThat(exc).hasMessageContaining("parameter count mismatch");
      }
    }
  }

  @Test
  void initParams() throws Exception {
    File tmpPath = File.createTempFile("jni_test", ".sqlite");
    //noinspection ResultOfMethodCallIgnored
    tmpPath.delete();
    tmpPath.deleteOnExit();

    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");

      parameters.put("uri", "file:" + tmpPath);

      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect();
          final AdbcStatement stmt = conn.createStatement()) {
        stmt.setSqlQuery("CREATE TABLE foo (v)");
        stmt.executeQuery().close();
      }

      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect();
          final AdbcStatement stmt = conn.createStatement()) {
        stmt.setSqlQuery("INSERT INTO foo VALUES (1), (2), (3)");
        stmt.executeQuery().close();
      }

      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect();
          final AdbcStatement stmt = conn.createStatement()) {
        stmt.setSqlQuery("SELECT * FROM foo ORDER BY v ASC");
        try (final AdbcStatement.QueryResult result = stmt.executeQuery()) {
          assertThat(result.getReader().loadNextBatch()).isTrue();
          assertThat(result.getReader().getVectorSchemaRoot().getRowCount()).isEqualTo(3);

          assertThat(result.getReader().loadNextBatch()).isFalse();
        }
      }
    }
  }

  void queryParams() throws Exception {
    final Schema paramSchema =
        new Schema(Collections.singletonList(Field.nullable("", Types.MinorType.BIGINT.getType())));
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");

      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect();
          final AdbcStatement stmt = conn.createStatement();
          final VectorSchemaRoot root = VectorSchemaRoot.create(paramSchema, allocator)) {
        ((BigIntVector) root.getVector(0)).setSafe(0, 41);
        ((BigIntVector) root.getVector(0)).setNull(1);
        root.setRowCount(2);

        stmt.setSqlQuery("SELECT 1 + ?");
        stmt.bind(root);

        try (final AdbcStatement.QueryResult result = stmt.executeQuery()) {
          assertThat(result.getReader().loadNextBatch()).isTrue();
          assertThat(result.getReader().getVectorSchemaRoot().getVector(0).getObject(0))
              .isEqualTo(42L);
          assertThat(result.getReader().getVectorSchemaRoot().getVector(0).getObject(1)).isNull();

          assertThat(result.getReader().loadNextBatch()).isFalse();
        }
      }
    }
  }
}
