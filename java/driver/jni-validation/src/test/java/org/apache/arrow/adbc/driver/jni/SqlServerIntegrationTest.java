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
import java.util.Objects;
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

class SqlServerIntegrationTest {
  public static final String URI_ENV = "ADBC_MSSQL_TEST_URI";
  static String URI = System.getenv(URI_ENV);

  BufferAllocator allocator;
  JniDriver driver;
  AdbcDatabase db;
  AdbcConnection conn;

  @BeforeAll
  static void beforeAll() {
    Assumptions.assumeFalse(
        URI == null || URI.isEmpty(),
        String.format("Must set %s to run MSSQL integration tests", URI_ENV));
  }

  @BeforeEach
  void beforeEach() throws Exception {
    System.err.println("Connecting to MSSQL with URI: " + URI);
    allocator = new RootAllocator();
    driver = new JniDriver(allocator);
    Map<String, Object> parameters = new HashMap<>();
    JniDriver.PARAM_DRIVER.set(parameters, "mssql");
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
      assertThat(infos.get(AdbcInfoCode.VENDOR_NAME.getValue())).isEqualTo("Microsoft SQL Server");
      assertThat(infos.get(AdbcInfoCode.DRIVER_NAME.getValue()))
          .isEqualTo("Columnar ADBC Driver for Microsoft SQL Server");

      assertThat(reader.loadNextBatch()).isFalse();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void connectionGetObjects() throws Exception {
    runSetup(
        "DROP TABLE IF EXISTS test_schema.foobar",
        "DROP SCHEMA IF EXISTS test_schema",
        "DROP TABLE IF EXISTS foobar",
        "CREATE SCHEMA test_schema",
        "CREATE TABLE test_schema.foobar (a INT)",
        "CREATE TABLE foobar (b VARCHAR)");

    try (var reader =
        conn.getObjects(
            AdbcConnection.GetObjectsDepth.ALL, null, "TEST_SCHEMA", null, null, null)) {
      var values =
          ArrowToJava.toObjects(reader, "catalog_db_schemas").stream()
              .filter(Objects::nonNull)
              .filter(x -> !((List<?>) x).isEmpty())
              .collect(Collectors.toList());
      var schemas = (List<?>) values.get(0);
      assertThat(schemas).isNotNull();
      var schema = (Map<String, ?>) schemas.get(0);
      assertThat(schema).extractingByKey("db_schema_name").isEqualTo("test_schema");
      var tables = (List<?>) schema.get("db_schema_tables");
      assertThat(tables).size().isEqualTo(1);
      var table = (Map<String, ?>) tables.get(0);
      assertThat(table).extractingByKey("table_name").isEqualTo("foobar");
      assertThat(table).extractingByKey("table_type").isEqualTo("BASE TABLE");
      var columns = (List<Map<String, ?>>) table.get("table_columns");
      assertThat(columns).singleElement().extracting("column_name").isEqualTo("a");
    }
  }

  @Test
  void connectionGetTableSchema() throws Exception {
    runSetup(
        "DROP TABLE IF EXISTS test_schema.foobar",
        "DROP SCHEMA IF EXISTS test_schema",
        "DROP TABLE IF EXISTS foobar",
        "CREATE SCHEMA test_schema",
        "CREATE TABLE test_schema.foobar (a INT)",
        "CREATE TABLE foobar (b VARCHAR)");

    // XXX: this appears to get the wrong table
    // assertSchema(conn.getTableSchema(null, "test_schema", "foobar"))
    //     .isEqualTo(new Schema(List.of(Field.nullable("a", Types.MinorType.INT.getType()))));
    assertSchema(conn.getTableSchema(null, null, "foobar"))
        .isEqualTo(new Schema(List.of(Field.nullable("b", Types.MinorType.VARCHAR.getType()))));
  }

  @Test
  void connectionTableTypes() throws Exception {
    try (ArrowReader reader = conn.getTableTypes()) {
      List<String> tableTypes = ArrowToJava.toStrings(reader, "table_type");
      assertThat(tableTypes).containsExactlyInAnyOrder("BASE TABLE", "VIEW");
    }
  }

  @Test
  void connectionReadOnly() {
    AdbcException e = assertThrows(AdbcException.class, () -> conn.getReadOnly());
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_FOUND);

    e = assertThrows(AdbcException.class, () -> conn.setReadOnly(true));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.INVALID_ARGUMENT);
  }

  @Test
  void bulkIngest() throws Exception {
    runSetup("DROP TABLE IF EXISTS foobar");

    final Schema schema =
        new Schema(
            List.of(
                Field.nullable("ndx", Types.MinorType.INT.getType()),
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
      stmt.setSqlQuery("SELECT value FROM foobar ORDER BY ndx");
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
        "DROP TABLE IF EXISTS test_schema.foobar",
        "DROP SCHEMA IF EXISTS test_schema",
        "DROP TABLE IF EXISTS foobar",
        "CREATE SCHEMA test_schema",
        "CREATE TABLE test_schema.foobar (a INT)",
        "CREATE TABLE foobar (b VARCHAR)");
    assertThat(conn.getCurrentCatalog()).isEqualTo("master");
    assertThat(conn.getCurrentDbSchema()).isEqualTo("dbo");

    AdbcException e = assertThrows(AdbcException.class, () -> conn.setCurrentCatalog("foobar"));
    assertThat(e).hasMessageContaining("Database 'foobar' does not exist");
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.INTERNAL);

    // MSSQL does not let you change the search path
    e = assertThrows(AdbcException.class, () -> conn.setCurrentDbSchema("test_schema"));
    assertThat(e.getStatus()).isEqualTo(AdbcStatusCode.NOT_IMPLEMENTED);

    try (AdbcStatement stmt = conn.createStatement()) {
      stmt.setSqlQuery("SELECT * FROM foobar");
      try (AdbcStatement.QueryResult result = stmt.executeQuery()) {
        assertSchema(result.getReader().getVectorSchemaRoot().getSchema())
            .isEqualTo(new Schema(List.of(Field.nullable("b", Types.MinorType.VARCHAR.getType()))));
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
                      Field.notNullable("THEANSWER", Types.MinorType.INT.getType()),
                      Field.notNullable("THEQUESTION", Types.MinorType.VARCHAR.getType()))));

      try (AdbcStatement.QueryResult result = stmt.executeQuery()) {
        assertThat(result.getReader().loadNextBatch()).isTrue();
        assertThat(result.getReader().loadNextBatch()).isFalse();
      }
    }
    // TODO(https://github.com/apache/arrow-adbc/issues/4239): test get parameter schema
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
          .hasMessageContaining("Cannot Prepare without a query");
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
      stmt.setSqlQuery("SELECT @p1 + 1 AS baz");
      // XXX: not supported due to parameter
      // assertSchema(stmt.executeSchema())
      //     .isEqualTo(new Schema(List.of(Field.nullable("baz", Types.MinorType.INT.getType()))));

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
      stmt.setSqlQuery("INSERT INTO foobar VALUES (@p1, @p2 * 2)");

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

      // XXX: appears to be a leak in the driver
      // AdbcStatement.UpdateResult result = stmt.executeUpdate();
      // assertThat(result.getAffectedRows()).isEqualTo(3);

      // iv.setSafe(0, 100);
      // biv.setSafe(0, 3);
      // vsr.setRowCount(1);

      // result = stmt.executeUpdate();
      // assertThat(result.getAffectedRows()).isEqualTo(1);

      // stmt.setSqlQuery("SELECT i FROM foobar ORDER BY j ASC");
      // try (AdbcStatement.QueryResult queryResult = stmt.executeQuery()) {
      //   var values = ArrowToJava.toIntegers(queryResult.getReader(), "i");
      //   assertThat(values).containsExactly(1, 42, null, 100);
      // }
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

    // XXX: the returned status code is inconsistent between different handle types
    assertThrows(
        AdbcException.class, () -> handle.setOption(new TypedKey<>("foo", String.class), "bar"));
    assertThrows(
        AdbcException.class, () -> handle.setOption(new TypedKey<>("foo", Integer.class), 42));
    assertThrows(
        AdbcException.class, () -> handle.setOption(new TypedKey<>("foo", Long.class), 42L));
    assertThrows(
        AdbcException.class, () -> handle.setOption(new TypedKey<>("foo", Boolean.class), true));
    assertThrows(
        AdbcException.class, () -> handle.setOption(new TypedKey<>("foo", Float.class), 3.14f));
    assertThrows(
        AdbcException.class, () -> handle.setOption(new TypedKey<>("foo", Double.class), 3.14));
    assertThrows(
        AdbcException.class,
        () -> handle.setOption(new TypedKey<>("foo", byte[].class), new byte[] {1, 2, 3}));
  }
}
