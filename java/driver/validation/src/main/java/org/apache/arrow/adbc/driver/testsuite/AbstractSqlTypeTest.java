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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test the Arrow type/value for each SQL type/value, for SQL-based databases. */
public class AbstractSqlTypeTest {
  /** Must be initialized by the subclass. */
  protected static SqlValidationQuirks quirks;

  protected BufferAllocator allocator;
  protected AdbcDatabase database;
  protected AdbcConnection connection;
  protected AdbcStatement statement;
  protected SqlTestUtil util;

  @BeforeEach
  public void beforeEach() throws Exception {
    Preconditions.checkNotNull(quirks, "Must initialize quirks in subclass with @BeforeAll");
    allocator = new RootAllocator();
    database = quirks.initDatabase(allocator);
    connection = database.connect();
    util = new SqlTestUtil(quirks);

    final String setupSql = getResource(this.getClass().getSimpleName() + ".sql");
    try (final AdbcStatement stmt = connection.createStatement()) {
      stmt.setSqlQuery(setupSql);
      stmt.executeUpdate();
    }
  }

  @AfterEach
  public void afterEach() throws Exception {
    AutoCloseables.close(connection, database, allocator);
  }

  protected static String getResource(final String name) throws Exception {
    try (InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(name)) {
      if (is == null) {
        throw new RuntimeException("Could not find resource " + name);
      }
      try (InputStreamReader isr = new InputStreamReader(is);
          BufferedReader reader = new BufferedReader(isr)) {
        return reader.lines().collect(Collectors.joining(System.lineSeparator()));
      }
    }
  }

  protected ArrowType assertValue(
      final String column, final Class<?> vectorType, final Object expectedValue) throws Exception {
    try (final AdbcStatement stmt = connection.createStatement()) {
      stmt.setSqlQuery("SELECT " + column + " FROM adbc_alltypes");
      try (AdbcStatement.QueryResult result = stmt.executeQuery()) {
        ArrowReader reader = result.getReader();
        assertThat(reader.loadNextBatch()).isTrue();
        assertThat(reader.getVectorSchemaRoot().getRowCount()).isEqualTo(2);
        FieldVector vector = reader.getVectorSchemaRoot().getVector(0);
        assertThat(vector).isInstanceOf(vectorType);
        assertThat(vector.getObject(0)).isEqualTo(expectedValue);
        assertThat(vector.getObject(1)).isNull();
        return vector.getField().getType();
      }
    }
  }

  @Test
  protected void bigintType() throws Exception {
    final Schema schema = connection.getTableSchema(null, null, "adbc_alltypes");
    assertThat(schema.findField("bigint_t").getType())
        .asInstanceOf(InstanceOfAssertFactories.type(ArrowType.Int.class))
        .extracting(ArrowType.Int::getBitWidth, ArrowType.Int::getIsSigned)
        .containsExactly(64, true);
  }

  @Test
  protected void bigintValue() throws Exception {
    assertValue("bigint_t", BigIntVector.class, 42L);
  }

  @Test
  protected void dateType() throws Exception {
    final Schema schema = connection.getTableSchema(null, null, "adbc_alltypes");
    assertThat(schema.findField("date_t").getType()).isEqualTo(new ArrowType.Date(DateUnit.DAY));
  }

  @Test
  protected void dateValue() throws Exception {
    assertValue("date_t", DateDayVector.class, 10957);
  }

  @Test
  protected void intType() throws Exception {
    final Schema schema = connection.getTableSchema(null, null, "adbc_alltypes");
    assertThat(schema.findField("int_t").getType())
        .asInstanceOf(InstanceOfAssertFactories.type(ArrowType.Int.class))
        .extracting(ArrowType.Int::getBitWidth, ArrowType.Int::getIsSigned)
        .containsExactly(32, true);
  }

  @Test
  protected void intValue() throws Exception {
    assertValue("int_t", IntVector.class, 42);
  }

  @Test
  protected void textType() throws Exception {
    final Schema schema = connection.getTableSchema(null, null, "adbc_alltypes");
    assertThat(schema.findField("text_t").getType()).isInstanceOf(ArrowType.Utf8.class);
  }

  @Test
  protected void textValue() throws Exception {
    assertValue("text_t", VarCharVector.class, new Text("foo"));
  }

  @Test
  protected void timestampWithoutTimeZoneType() throws Exception {
    final Schema schema = connection.getTableSchema(null, null, "adbc_alltypes");
    assertThat(schema.findField("timestamp_without_time_zone_t").getType())
        .isEqualTo(new ArrowType.Timestamp(quirks.defaultTimestampUnit(), null));
  }

  @Test
  protected void timestampWithoutTimeZoneValue() throws Exception {
    final ArrowType type;
    switch (quirks.defaultTimestampUnit()) {
      case SECOND:
        type = assertValue("timestamp_without_time_zone_t", TimeStampVector.class, 946_782_245L);
        break;
      case MILLISECOND:
        type =
            assertValue("timestamp_without_time_zone_t", TimeStampVector.class, 946_782_245_123L);
        break;
      case MICROSECOND:
        type =
            assertValue(
                "timestamp_without_time_zone_t", TimeStampVector.class, 946_782_245_123_000L);
        break;
      case NANOSECOND:
        type =
            assertValue(
                "timestamp_without_time_zone_t", TimeStampVector.class, 946_782_245_123_000_000L);
        break;
      default:
        throw new UnsupportedOperationException();
    }
    assertThat(type).isEqualTo(new ArrowType.Timestamp(quirks.defaultTimestampUnit(), null));
  }

  @Test
  protected void timestampWithTimeZoneType() throws Exception {
    final Schema schema = connection.getTableSchema(null, null, "adbc_alltypes");
    assertThat(schema.findField("timestamp_with_time_zone_t").getType())
        .isEqualTo(new ArrowType.Timestamp(quirks.defaultTimestampUnit(), "UTC"));
  }

  @Test
  protected void timestampWithTimeZoneValue() throws Exception {
    switch (quirks.defaultTimestampUnit()) {
      case SECOND:
        assertValue("timestamp_with_time_zone_t", TimeStampSecTZVector.class, 946_760_645L);
        break;
      case MILLISECOND:
        assertValue("timestamp_with_time_zone_t", TimeStampMilliTZVector.class, 946_760_645_123L);
        break;
      case MICROSECOND:
        assertValue(
            "timestamp_with_time_zone_t", TimeStampMicroTZVector.class, 946_760_645_123_000L);
        break;
      case NANOSECOND:
        assertValue(
            "timestamp_with_time_zone_t", TimeStampNanoTZVector.class, 946_760_645_123_000_000L);
        break;
    }
  }
}
