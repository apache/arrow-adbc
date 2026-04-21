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

import static org.apache.arrow.adbc.driver.testsuite.ArrowAssertions.assertSchema;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.AdbcOptions;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.adbc.driver.testsuite.ArrowToJava;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PostgresIntegrationTest {
  public static final String URI_ENV = "ADBC_POSTGRESQL_TEST_URI";
  static String URI = System.getenv(URI_ENV);

  BufferAllocator allocator;
  JniDriver driver;
  AdbcDatabase db;
  AdbcConnection conn;

  @BeforeAll
  static void beforeAll() {
    Assumptions.assumeFalse(
        URI == null || URI.isEmpty(),
        String.format("Must set %s to run Postgres integration tests", URI_ENV));
  }

  @BeforeEach
  void beforeEach() throws Exception {
    System.err.println("Connecting to PostgreSQL with URI: " + URI);
    allocator = new RootAllocator();
    driver = new JniDriver(allocator);
    Map<String, Object> parameters = new HashMap<>();
    JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_postgresql");
    AdbcDriver.PARAM_URI.set(parameters, URI);
    db = driver.open(parameters);
    conn = db.connect();
  }

  @AfterEach
  void afterEach() throws Exception {
    conn.close();
    db.close();
    allocator.close();
  }

  @Test
  void simple() throws Exception {
    try (var stmt = conn.createStatement()) {
      stmt.setSqlQuery("SELECT 1 + 1 AS sum");
      try (var reader = stmt.executeQuery()) {
        assertThat(reader.getReader().loadNextBatch()).isTrue();
        assertThat(reader.getReader().getVectorSchemaRoot().getVector("sum").getObject(0))
            .isEqualTo(2);
      }
    }
  }

  @Test
  void options() throws Exception {
    testOptions(db);
    testOptions(conn);
    try (AdbcStatement stmt = conn.createStatement()) {
      testOptions(stmt);
    }
  }

  @Test
  void connectionGetInfo() throws Exception {
    try (ArrowReader reader = conn.getInfo()) {
      assertThat(reader.loadNextBatch()).isTrue();

      var codes = ArrowToJava.toIntegers(reader.getVectorSchemaRoot().getVector("info_name"));
      var values = ArrowToJava.toObjects(reader.getVectorSchemaRoot().getVector("info_value"));
      var infos =
          IntStream.range(0, codes.size())
              .mapToObj(i -> Map.entry(codes.get(i), values.get(i)))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      assertThat(codes).contains(AdbcInfoCode.VENDOR_NAME.getValue());
      assertThat(codes).contains(AdbcInfoCode.VENDOR_VERSION.getValue());
      assertThat(codes).contains(AdbcInfoCode.DRIVER_NAME.getValue());
      assertThat(codes).contains(AdbcInfoCode.DRIVER_VERSION.getValue());
      assertThat(infos.get(AdbcInfoCode.VENDOR_NAME.getValue())).isEqualTo("PostgreSQL");
      assertThat(infos.get(AdbcInfoCode.DRIVER_NAME.getValue()))
          .isEqualTo("ADBC PostgreSQL Driver");

      assertThat(reader.loadNextBatch()).isFalse();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void connectionGetObjects() throws Exception {
    runSetup(
        "DROP SCHEMA IF EXISTS test_schema CASCADE",
        "DROP TABLE IF EXISTS public.foobar",
        "CREATE SCHEMA test_schema",
        "CREATE TABLE test_schema.foobar (a INT)",
        "CREATE TABLE public.foobar (b TEXT)");

    try (var reader =
        conn.getObjects(
            AdbcConnection.GetObjectsDepth.ALL, null, "test_schema", null, null, null)) {
      var values = ArrowToJava.toObjects(reader, "catalog_db_schemas");
      var schemas = (List<?>) values.get(0);
      var schema = (Map<String, ?>) schemas.get(0);
      assertThat(schema).extractingByKey("db_schema_name").isEqualTo("test_schema");
      var tables = (List<?>) schema.get("db_schema_tables");
      assertThat(tables).size().isEqualTo(1);
      var table = (Map<String, ?>) tables.get(0);
      assertThat(table).extractingByKey("table_name").isEqualTo("foobar");
      assertThat(table).extractingByKey("table_type").isEqualTo("table");
      var columns = (List<Map<String, ?>>) table.get("table_columns");
      assertThat(columns).singleElement().extracting("column_name").isEqualTo("a");
    }
  }

  @Test
  void connectionGetTableSchema() throws Exception {
    runSetup(
        "DROP SCHEMA IF EXISTS test_schema CASCADE",
        "DROP TABLE IF EXISTS public.foobar",
        "CREATE SCHEMA test_schema",
        "CREATE TABLE test_schema.foobar (a INT)",
        "CREATE TABLE public.foobar (b TEXT)");

    assertSchema(conn.getTableSchema(null, "test_schema", "foobar"))
        .isEqualTo(new Schema(List.of(Field.nullable("a", Types.MinorType.INT.getType()))));
    assertSchema(conn.getTableSchema(null, null, "foobar"))
        .isEqualTo(new Schema(List.of(Field.nullable("b", Types.MinorType.VARCHAR.getType()))));
  }

  @Test
  void connectionTableTypes() throws Exception {
    try (ArrowReader reader = conn.getTableTypes()) {
      List<String> tableTypes = ArrowToJava.toStrings(reader, "table_type");
      assertThat(tableTypes)
          .containsExactlyInAnyOrder(
              "partitioned_table",
              "foreign_table",
              "toast_table",
              "materialized_view",
              "view",
              "table");
    }
  }

  @Test
  void connectionReadOnly() {
    AdbcException e = assertThrows(AdbcException.class, () -> conn.getReadOnly());
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_FOUND);

    e = assertThrows(AdbcException.class, () -> conn.setReadOnly(true));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_IMPLEMENTED);
  }

  @Test
  void bulkIngest() throws Exception {
    runSetup("DROP TABLE IF EXISTS foobar");

    final Schema schema =
        new Schema(
            List.of(
                Field.nullable("index", Types.MinorType.INT.getType()),
                Field.nullable("value", Types.MinorType.VARCHAR.getType())));
    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator)) {
      IntVector iv = (IntVector) vsr.getVector(0);
      VarCharVector vv = (VarCharVector) vsr.getVector(1);

      try (AdbcStatement stmt = conn.bulkIngest("foobar", BulkIngestMode.CREATE)) {
        iv.setSafe(0, 1);
        iv.setSafe(1, 2);
        vv.setNull(0);
        vv.setSafe(1, "foobar".getBytes(StandardCharsets.UTF_8));
        vsr.setRowCount(2);

        stmt.bind(vsr);
        assertThat(stmt.executeUpdate().getAffectedRows()).isEqualTo(2);
      }

      try (AdbcStatement stmt = conn.bulkIngest("foobar", BulkIngestMode.REPLACE)) {
        iv.setSafe(0, 1);
        iv.setSafe(1, 2);
        vv.setSafe(0, "".getBytes(StandardCharsets.UTF_8));
        vv.setSafe(1, "testtest".getBytes(StandardCharsets.UTF_8));
        vsr.setRowCount(2);

        stmt.bind(vsr);
        assertThat(stmt.executeUpdate().getAffectedRows()).isEqualTo(2);
      }

      try (AdbcStatement stmt = conn.bulkIngest("foobar", BulkIngestMode.APPEND)) {
        iv.setSafe(0, 3);
        iv.setSafe(1, 4);
        iv.setSafe(2, 5);
        vv.setSafe(0, "spam".getBytes(StandardCharsets.UTF_8));
        vv.setNull(1);
        vv.setSafe(2, "eggs".getBytes(StandardCharsets.UTF_8));
        vsr.setRowCount(3);

        stmt.bind(vsr);
        assertThat(stmt.executeUpdate().getAffectedRows()).isEqualTo(3);
      }

      try (AdbcStatement stmt = conn.bulkIngest("foobar", BulkIngestMode.CREATE_APPEND)) {
        iv.setSafe(0, 6);
        iv.setSafe(1, 7);
        iv.setSafe(2, 8);
        vv.setSafe(0, "spam".getBytes(StandardCharsets.UTF_8));
        vv.setNull(1);
        vv.setNull(2);
        vsr.setRowCount(3);

        stmt.bind(vsr);
        assertThat(stmt.executeUpdate().getAffectedRows()).isEqualTo(3);
      }
    }

    try (AdbcStatement stmt = conn.createStatement()) {
      stmt.setSqlQuery("SELECT value FROM foobar ORDER BY index");
      try (var result = stmt.executeQuery()) {
        var values = ArrowToJava.toStrings(result.getReader(), "value");
        assertThat(values)
            .containsExactly("", "testtest", "spam", null, "eggs", "spam", null, null);
      }
    }
  }

  @Test
  void currentCatalogSchema() throws Exception {
    runSetup(
        "DROP SCHEMA IF EXISTS test_schema CASCADE",
        "DROP TABLE IF EXISTS public.foobar",
        "CREATE SCHEMA test_schema",
        "CREATE TABLE test_schema.foobar (a INT)",
        "CREATE TABLE public.foobar (b TEXT)");
    assertThat(conn.getCurrentCatalog()).isEqualTo("postgres");
    assertThat(conn.getCurrentDbSchema()).isEqualTo("public");

    AdbcException e = assertThrows(AdbcException.class, () -> conn.setCurrentCatalog("foobar"));
    assertThat(e).hasMessageContaining("Unknown option");
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_IMPLEMENTED);

    conn.setCurrentDbSchema("test_schema");
    try (AdbcStatement stmt = conn.createStatement()) {
      stmt.setSqlQuery("SELECT * FROM foobar");
      try (AdbcStatement.QueryResult result = stmt.executeQuery()) {
        assertSchema(result.getReader().getVectorSchemaRoot().getSchema())
            .isEqualTo(new Schema(List.of(Field.nullable("a", Types.MinorType.INT.getType()))));
      }
    }
  }

  @Test
  void transactions() throws Exception {}

  @Test
  void selectQuery() throws Exception {
    try (var stmt = conn.createStatement()) {
      stmt.setSqlQuery("SELECT 42 AS THEANSWER, 'meaning of life' AS THEQUESTION");

      assertSchema(stmt.executeSchema())
          .isEqualTo(
              new Schema(
                  List.of(
                      Field.nullable("theanswer", Types.MinorType.INT.getType()),
                      Field.nullable("thequestion", Types.MinorType.VARCHAR.getType()))));

      try (AdbcStatement.QueryResult result = stmt.executeQuery()) {
        assertThat(result.getReader().loadNextBatch()).isTrue();
        assertThat(result.getReader().loadNextBatch()).isFalse();
      }

      assertThat(stmt.getParameterSchema().getFields()).isEmpty();
    }

    try (var stmt = conn.createStatement()) {
      stmt.setSqlQuery("SELECT $1 || 'foo'");
      assertSchema(stmt.getParameterSchema())
          .isEqualTo(new Schema(List.of(Field.nullable("$1", Types.MinorType.VARCHAR.getType()))));
    }
  }

  @Test
  void cancelQuery() throws Exception {
    // There's nothing really we can test reliably; it is wired up but we'd need a long-running
    // query and a reliable way to start the cancel at the right time
    Schema schema = new Schema(List.of(Field.nullable("$1", Types.MinorType.VARCHAR.getType())));
    try (var stmt = conn.createStatement();
        var vsr = VectorSchemaRoot.create(schema, allocator)) {
      var vcv = (VarCharVector) vsr.getVector(0);
      vcv.setSafe(0, "test".getBytes(StandardCharsets.UTF_8));
      vcv.setSafe(1, "bar".getBytes(StandardCharsets.UTF_8));

      stmt.setSqlQuery("SELECT CAST($1 AS VARCHAR) || 'foo'");
      stmt.bind(vsr);

      try (AdbcStatement.QueryResult result = stmt.executeQuery()) {
        stmt.cancel();
        //noinspection StatementWithEmptyBody
        while (result.getReader().loadNextBatch()) {}
      }
    }
  }

  @Test
  void cancelConnection() throws Exception {
    // There's nothing really we can test reliably
    conn.cancel();
  }

  @Test
  void updateQuery() throws Exception {
    runSetup("DROP TABLE IF EXISTS foobar", "CREATE TABLE foobar (i INT)");

    try (var stmt = conn.createStatement()) {
      stmt.setSqlQuery("INSERT INTO foobar VALUES (1), (2)");
      AdbcStatement.UpdateResult result = stmt.executeUpdate();
      assertThat(result.getAffectedRows()).isEqualTo(2);
    }
  }

  @Test
  void prepare() throws Exception {
    try (var stmt = conn.createStatement()) {
      assertThat(assertThrows(AdbcException.class, stmt::prepare))
          .hasMessageContaining("Must SetSqlQuery() before Prepare()");
    }
  }

  @Test
  void prepareSelectQuery() throws Exception {
    runSetup(
        "DROP TABLE IF EXISTS foobar",
        "CREATE TABLE foobar (i INT)",
        "INSERT INTO foobar VALUES (1), (2)");

    try (var stmt = conn.createStatement()) {
      stmt.setSqlQuery("SELECT i + 2 AS baz FROM foobar ORDER BY baz ASC");
      stmt.prepare();

      assertSchema(stmt.executeSchema())
          .isEqualTo(new Schema(List.of(Field.nullable("baz", Types.MinorType.INT.getType()))));

      try (AdbcStatement.QueryResult result = stmt.executeQuery()) {
        var values = ArrowToJava.toIntegers(result.getReader(), "baz");
        assertThat(values).containsExactly(3, 4);
      }

      try (AdbcStatement.QueryResult result = stmt.executeQuery()) {
        var values = ArrowToJava.toIntegers(result.getReader(), "baz");
        assertThat(values).containsExactly(3, 4);
      }
    }
  }

  @Test
  void prepareUpdateQuery() throws Exception {
    runSetup("DROP TABLE IF EXISTS foobar", "CREATE TABLE foobar (i INT)");

    try (var stmt = conn.createStatement()) {
      stmt.setSqlQuery("INSERT INTO foobar VALUES (1), (2)");
      stmt.prepare();

      AdbcStatement.UpdateResult result = stmt.executeUpdate();
      assertThat(result.getAffectedRows()).isEqualTo(2);

      result = stmt.executeUpdate();
      assertThat(result.getAffectedRows()).isEqualTo(2);
    }
  }

  @Test
  void bindSelectQuery() throws Exception {
    Schema schema = new Schema(List.of(Field.nullable("$1", Types.MinorType.INT.getType())));
    try (var stmt = conn.createStatement();
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator)) {
      stmt.setSqlQuery("SELECT $1 + 1 AS baz");
      assertSchema(stmt.executeSchema())
          .isEqualTo(new Schema(List.of(Field.nullable("baz", Types.MinorType.INT.getType()))));

      var iv = (IntVector) vsr.getVector(0);
      iv.setSafe(0, 1);
      iv.setSafe(1, 42);
      iv.setNull(2);
      vsr.setRowCount(3);
      stmt.bind(vsr);

      try (AdbcStatement.QueryResult result = stmt.executeQuery()) {
        var values = ArrowToJava.toIntegers(result.getReader(), "baz");
        assertThat(values).containsExactly(2, 43, null);
      }

      iv.setNull(0);
      iv.setSafe(1, 401);
      iv.setSafe(2, 200);
      iv.setSafe(3, 503);
      vsr.setRowCount(4);
      try (AdbcStatement.QueryResult result = stmt.executeQuery()) {
        var values = ArrowToJava.toIntegers(result.getReader(), "baz");
        assertThat(values).containsExactly(null, 402, 201, 504);
      }
    }
  }

  @Test
  void bindUpdateQuery() throws Exception {
    runSetup("DROP TABLE IF EXISTS foobar", "CREATE TABLE foobar (i INT, j BIGINT)");

    Schema schema =
        new Schema(
            List.of(
                Field.nullable("$1", Types.MinorType.INT.getType()),
                Field.nullable("$2", Types.MinorType.BIGINT.getType())));
    try (var stmt = conn.createStatement();
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator)) {
      stmt.setSqlQuery("INSERT INTO foobar VALUES ($1, $2 * 2)");

      var iv = (IntVector) vsr.getVector(0);
      iv.setSafe(0, 1);
      iv.setSafe(1, 42);
      iv.setNull(2);

      var biv = (BigIntVector) vsr.getVector(1);
      biv.setSafe(0, 0);
      biv.setSafe(1, 1);
      biv.setSafe(2, 2);

      vsr.setRowCount(3);
      stmt.bind(vsr);

      AdbcStatement.UpdateResult result = stmt.executeUpdate();
      assertThat(result.getAffectedRows()).isEqualTo(3);

      iv.setSafe(0, 100);
      biv.setSafe(0, 3);
      vsr.setRowCount(1);

      result = stmt.executeUpdate();
      assertThat(result.getAffectedRows()).isEqualTo(1);

      stmt.setSqlQuery("SELECT i FROM foobar ORDER BY j ASC");
      try (AdbcStatement.QueryResult queryResult = stmt.executeQuery()) {
        var values = ArrowToJava.toIntegers(queryResult.getReader(), "i");
        assertThat(values).containsExactly(1, 42, null, 100);
      }
    }
  }

  void runSetup(String... sql) throws Exception {
    try (var stmt = conn.createStatement()) {
      for (String s : sql) {
        stmt.setSqlQuery(s);
        stmt.executeUpdate();
      }
    }
  }

  void testOptions(AdbcOptions handle) {
    AdbcException e;

    e =
        assertThrows(
            AdbcException.class, () -> handle.getOption(new TypedKey<>("foo", String.class)));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_FOUND);
    e =
        assertThrows(
            AdbcException.class, () -> handle.getOption(new TypedKey<>("foo", Integer.class)));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_FOUND);
    e =
        assertThrows(
            AdbcException.class, () -> handle.getOption(new TypedKey<>("foo", Long.class)));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_FOUND);
    e =
        assertThrows(
            AdbcException.class, () -> handle.getOption(new TypedKey<>("foo", Boolean.class)));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_FOUND);
    e =
        assertThrows(
            AdbcException.class, () -> handle.getOption(new TypedKey<>("foo", Float.class)));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_FOUND);
    e =
        assertThrows(
            AdbcException.class, () -> handle.getOption(new TypedKey<>("foo", Double.class)));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_FOUND);
    e =
        assertThrows(
            AdbcException.class, () -> handle.getOption(new TypedKey<>("foo", byte[].class)));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_FOUND);

    e =
        assertThrows(
            AdbcException.class,
            () -> handle.setOption(new TypedKey<>("foo", String.class), "bar"));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_IMPLEMENTED);
    e =
        assertThrows(
            AdbcException.class, () -> handle.setOption(new TypedKey<>("foo", Integer.class), 42));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_IMPLEMENTED);
    e =
        assertThrows(
            AdbcException.class, () -> handle.setOption(new TypedKey<>("foo", Long.class), 42L));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_IMPLEMENTED);
    e =
        assertThrows(
            AdbcException.class,
            () -> handle.setOption(new TypedKey<>("foo", Boolean.class), true));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_IMPLEMENTED);
    e =
        assertThrows(
            AdbcException.class, () -> handle.setOption(new TypedKey<>("foo", Float.class), 3.14f));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_IMPLEMENTED);
    e =
        assertThrows(
            AdbcException.class, () -> handle.setOption(new TypedKey<>("foo", Double.class), 3.14));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_IMPLEMENTED);
    e =
        assertThrows(
            AdbcException.class,
            () -> handle.setOption(new TypedKey<>("foo", byte[].class), new byte[] {1, 2, 3}));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_IMPLEMENTED);
  }
}
