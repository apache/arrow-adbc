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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
  void loadManifest(@TempDir Path tempDir) throws Exception {
    Path tempFile = Files.createFile(tempDir.resolve("mydriver.toml"));
    // TODO(https://github.com/apache/arrow-adbc/issues/3536): use proper multiline string
    Files.write(
        tempFile,
        String.join("\n", "manifest_version = 1", "[Driver]", "shared = \"adbc_driver_sqlite\"")
            .getBytes(StandardCharsets.UTF_8));

    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "mydriver");
      JniDriver.PARAM_MANIFEST_SEARCH_PATH.set(parameters, tempDir.toString());
      driver.open(parameters).close();
    }
  }

  @Test
  void loadProfile(@TempDir Path tempDir) throws Exception {
    Path tempFile = Files.createFile(tempDir.resolve("myprofile.toml"));
    // TODO(https://github.com/apache/arrow-adbc/issues/3536): use proper multiline string
    Files.write(
        tempFile,
        String.join(
                "\n",
                "profile_version = 1",
                "driver = \"adbc_driver_sqlite\"",
                "[Options]",
                "adbc.sqlite.query.batch_rows = 4242")
            .getBytes(StandardCharsets.UTF_8));
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_PROFILE.set(parameters, "myprofile");
      JniDriver.PARAM_PROFILE_SEARCH_PATH.set(parameters, tempDir.toString());
      try (final AdbcDatabase db = driver.open(parameters)) {
        assertThat(db.getOption(new TypedKey<>("adbc.sqlite.query.batch_rows", Long.class)))
            .isEqualTo(4242L);
      }
    }
  }

  @Test
  void loadUri() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      AdbcDriver.PARAM_URI.set(parameters, "adbc_driver_sqlite://");
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

  @Test
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
        root.getVector(0).setNull(1);
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

  @Test
  void executeUpdate() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");

      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect()) {
        // Create table
        try (final AdbcStatement stmt = conn.createStatement()) {
          stmt.setSqlQuery("CREATE TABLE test_update (id INTEGER, name TEXT)");
          stmt.executeUpdate();
        }

        // Insert rows
        try (final AdbcStatement stmt = conn.createStatement()) {
          stmt.setSqlQuery("INSERT INTO test_update VALUES (1, 'a'), (2, 'b'), (3, 'c')");
          AdbcStatement.UpdateResult result = stmt.executeUpdate();
          assertThat(result.getAffectedRows()).isEqualTo(3L);
        }

        // Verify data was inserted
        try (final AdbcStatement stmt = conn.createStatement()) {
          stmt.setSqlQuery("SELECT COUNT(*) FROM test_update");
          try (final AdbcStatement.QueryResult result = stmt.executeQuery()) {
            assertThat(result.getReader().loadNextBatch()).isTrue();
            assertThat(result.getReader().getVectorSchemaRoot().getVector(0).getObject(0))
                .isEqualTo(3L);
          }
        }
      }
    }
  }

  @Test
  void preparedStatement() throws Exception {
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
        stmt.setSqlQuery("SELECT 1 + ?");
        stmt.prepare();

        ((BigIntVector) root.getVector(0)).setSafe(0, 41);
        root.setRowCount(1);
        stmt.bind(root);

        try (final AdbcStatement.QueryResult result = stmt.executeQuery()) {
          assertThat(result.getReader().loadNextBatch()).isTrue();
          assertThat(result.getReader().getVectorSchemaRoot().getVector(0).getObject(0))
              .isEqualTo(42L);
        }
      }
    }
  }

  @Test
  void getObjects() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");

      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect()) {
        // Create a table so there's something to find
        try (final AdbcStatement stmt = conn.createStatement()) {
          stmt.setSqlQuery("CREATE TABLE get_objects_test (id INTEGER, name TEXT)");
          stmt.executeUpdate();
        }

        try (final ArrowReader reader =
            conn.getObjects(AdbcConnection.GetObjectsDepth.ALL, null, null, null, null, null)) {
          assertThat(reader.loadNextBatch()).isTrue();
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          assertThat(root.getRowCount()).isGreaterThan(0);
          // First field should be catalog_name
          assertThat(root.getSchema().getFields().get(0).getName()).isEqualTo("catalog_name");
        }
      }
    }
  }

  @Test
  void bulkIngest() throws Exception {
    final Schema schema =
        new Schema(
            Collections.singletonList(Field.nullable("v", Types.MinorType.BIGINT.getType())));
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");

      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect()) {
        // Bulk ingest with CREATE mode
        try (final AdbcStatement stmt = conn.bulkIngest("bulk_test", BulkIngestMode.CREATE);
            final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
          ((BigIntVector) root.getVector(0)).setSafe(0, 1);
          ((BigIntVector) root.getVector(0)).setSafe(1, 2);
          ((BigIntVector) root.getVector(0)).setSafe(2, 3);
          root.setRowCount(3);

          stmt.bind(root);
          AdbcStatement.UpdateResult result = stmt.executeUpdate();
          assertThat(result.getAffectedRows()).isEqualTo(3L);
        }

        // Verify data
        try (final AdbcStatement stmt = conn.createStatement()) {
          stmt.setSqlQuery("SELECT * FROM bulk_test ORDER BY v ASC");
          try (final AdbcStatement.QueryResult result = stmt.executeQuery()) {
            assertThat(result.getReader().loadNextBatch()).isTrue();
            assertThat(result.getReader().getVectorSchemaRoot().getRowCount()).isEqualTo(3);
          }
        }
      }
    }
  }

  @Test
  void commit() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");
      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect()) {
        try (final AdbcStatement stmt = conn.createStatement()) {
          stmt.setSqlQuery("DROP TABLE IF EXISTS foobar");
          stmt.executeUpdate();
        }

        assertThat(conn.getAutoCommit()).isTrue();
        // not supported by sqlite driver
        // assertThat(conn.getIsolationLevel()).isEqualTo(IsolationLevel.SERIALIZABLE);
        conn.setAutoCommit(false);
        assertThat(conn.getAutoCommit()).isFalse();

        try (final AdbcStatement stmt = conn.createStatement()) {
          stmt.setSqlQuery("CREATE TABLE foobar (v)");
          stmt.executeUpdate();

          stmt.setSqlQuery("SELECT * FROM foobar");
          try (AdbcStatement.QueryResult result = stmt.executeQuery()) {
            result.getReader().loadNextBatch();
          }
        }

        conn.rollback();

        try (final AdbcStatement stmt = conn.createStatement()) {
          stmt.setSqlQuery("SELECT * FROM foobar");
          AdbcException e = assertThrows(AdbcException.class, stmt::executeQuery);
          assertThat(e).hasMessageContaining("no such table: foobar");
        }

        try (final AdbcStatement stmt = conn.createStatement()) {
          stmt.setSqlQuery("CREATE TABLE foobar (v)");
          stmt.executeUpdate();
        }

        conn.commit();

        try (final AdbcStatement stmt = conn.createStatement()) {
          stmt.setSqlQuery("SELECT * FROM foobar");
          try (AdbcStatement.QueryResult result = stmt.executeQuery()) {
            result.getReader().loadNextBatch();
          }
        }
      }
    }
  }

  @Test
  void currentCatalog() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");
      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect()) {
        // SQLite only has catalogs and cannot set the search path
        assertThat(conn.getCurrentCatalog()).isEqualTo("main");
      }
    }
  }

  @Test
  void getSetOption() throws Exception {
    TypedKey<Integer> batchRowsInt = new TypedKey<>("adbc.sqlite.query.batch_rows", Integer.class);
    TypedKey<Long> batchRowsLong = new TypedKey<>("adbc.sqlite.query.batch_rows", Long.class);
    TypedKey<Boolean> bindByName = new TypedKey<>("adbc.statement.bind_by_name", Boolean.class);
    TypedKey<String> bindByNameString = new TypedKey<>("adbc.statement.bind_by_name", String.class);
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");
      try (final AdbcDatabase db = driver.open(parameters)) {
        assertThat(db.getOption(batchRowsInt)).isEqualTo(1024);
        assertThat(db.getOption(batchRowsLong)).isEqualTo(1024L);
        assertThat(db.getOption(AdbcDriver.PARAM_URI))
            .isEqualTo("file:adbc_driver_sqlite?mode=memory&cache=shared");

        try (final AdbcConnection conn = db.connect();
            final AdbcStatement stmt = conn.createStatement()) {
          assertThat(conn.getOption(batchRowsInt)).isEqualTo(1024);
          assertThat(conn.getOption(batchRowsLong)).isEqualTo(1024L);

          assertThat(stmt.getOption(batchRowsInt)).isEqualTo(1024);
          assertThat(stmt.getOption(batchRowsLong)).isEqualTo(1024L);
          stmt.setOption(batchRowsLong, 42L);
          assertThat(stmt.getOption(batchRowsLong)).isEqualTo(42L);
          assertThat(stmt.getOption(bindByName)).isFalse();
          assertThat(stmt.getOption(bindByNameString)).isEqualTo("false");
          stmt.setOption(bindByName, true);
          assertThat(stmt.getOption(bindByName)).isTrue();
          assertThat(stmt.getOption(bindByNameString)).isEqualTo("true");
        }
      }
    }
  }

  static class GetSetOptionFailCase {
    @SuppressWarnings("rawtypes")
    final TypedKey key;

    final Object value;
    final String message;

    GetSetOptionFailCase(TypedKey<?> key, Object value, String message) {
      this.key = key;
      this.value = value;
      this.message = message;
    }

    @Override
    public String toString() {
      String v;
      if (value == null) {
        v = "(NULL)";
      } else if (value instanceof byte[]) {
        v = Arrays.toString((byte[]) value);
      } else {
        v = value.toString();
      }
      return "key=" + key.getKey() + ", value=" + v;
    }
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource("getSetOptionFailProvider")
  void getSetOptionFailDatabase(GetSetOptionFailCase testCase) throws Exception {
    // These will fail; we don't have a driver that supports an example of every type
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");
      try (final AdbcDatabase db = driver.open(parameters)) {
        AdbcException e;
        //noinspection unchecked
        e = assertThrows(AdbcException.class, () -> db.setOption(testCase.key, testCase.value));
        assertThat(e).hasMessageContaining(testCase.message);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource("getSetOptionFailProvider")
  void getSetOptionFailConnection(GetSetOptionFailCase testCase) throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");
      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect()) {
        AdbcException e;
        //noinspection unchecked
        e = assertThrows(AdbcException.class, () -> conn.setOption(testCase.key, testCase.value));
        assertThat(e).hasMessageContaining(testCase.message);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource("getSetOptionFailProvider")
  void getSetOptionFailStatement(GetSetOptionFailCase testCase) throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");
      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect();
          final AdbcStatement stmt = conn.createStatement()) {
        AdbcException e;
        //noinspection unchecked
        e = assertThrows(AdbcException.class, () -> stmt.setOption(testCase.key, testCase.value));
        assertThat(e).hasMessageContaining(testCase.message);
      }
    }
  }

  static Stream<GetSetOptionFailCase> getSetOptionFailProvider() {
    return Stream.of(
        new GetSetOptionFailCase(new TypedKey<>("unknown", Integer.class), 2048, "unknown=2048"),
        new GetSetOptionFailCase(new TypedKey<>("unknown", Long.class), 2048L, "unknown=2048"),
        new GetSetOptionFailCase(new TypedKey<>("unknown", Float.class), 2048f, "unknown=2048.0"),
        new GetSetOptionFailCase(new TypedKey<>("unknown", Double.class), 2048d, "unknown=2048.0"),
        new GetSetOptionFailCase(
            new TypedKey<>("unknown", String.class), "foobar", "unknown='foobar'"),
        new GetSetOptionFailCase(new TypedKey<>("unknown", String.class), null, "unknown=(NULL)"),
        new GetSetOptionFailCase(
            new TypedKey<>("unknown", Boolean.class), false, "unknown='false'"),
        new GetSetOptionFailCase(new TypedKey<>("unknown", Boolean.class), true, "unknown='true'"),
        new GetSetOptionFailCase(
            new TypedKey<>("unknown", byte[].class), new byte[] {0, 42, 0}, "unknown=(3 bytes)"),
        new GetSetOptionFailCase(new TypedKey<>("unknown", byte[].class), null, "unknown=(NULL)"));
  }
}
