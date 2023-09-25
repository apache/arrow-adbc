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
package org.apache.arrow.adbc.driver.jdbc.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.StandardStatistics;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class StatisticsTest {
  static PostgresqlQuirks quirks;

  @BeforeAll
  static void beforeAll() {
    quirks = new PostgresqlQuirks();
  }

  @Test
  void adbc() throws Exception {
    try (Connection connection = quirks.getJdbcConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate("DROP TABLE IF EXISTS adbcpkeytest");
      statement.executeUpdate("CREATE TABLE adbcpkeytest (key SERIAL PRIMARY KEY, value INT)");
      statement.executeUpdate("INSERT INTO adbcpkeytest (value) VALUES (0), (1), (2)");
      statement.executeUpdate("ANALYZE adbcpkeytest");
    }

    try (BufferAllocator allocator = new RootAllocator();
        AdbcDatabase database = quirks.initDatabase(allocator);
        AdbcConnection connection = database.connect();
        ArrowReader reader = connection.getStatistics(null, null, "adbcpkeytest", true)) {
      assertThat(reader.loadNextBatch()).isTrue();
      VectorSchemaRoot vsr = reader.getVectorSchemaRoot();
      assertThat(vsr.getRowCount()).isEqualTo(1);

      ListVector catalogDbSchemas = (ListVector) vsr.getVector(1);
      assertThat(catalogDbSchemas.getValueCount()).isEqualTo(1);

      StructVector catalogDbSchema = (StructVector) catalogDbSchemas.getDataVector();
      ListVector dbSchemaStatistics = (ListVector) catalogDbSchema.getVectorById(1);
      assertThat(dbSchemaStatistics.getValueCount()).isEqualTo(1);

      @SuppressWarnings("unchecked")
      Map<String, Object> statistic = (Map<String, Object>) dbSchemaStatistics.getObject(0).get(0);
      assertThat(statistic)
          .contains(
              entry("table_name", new Text("adbcpkeytest")),
              entry("statistic_key", StandardStatistics.DISTINCT_COUNT.getKey()),
              entry("statistic_value", 3L));

      assertThat(reader.loadNextBatch()).isFalse();
    }
  }

  /** Validate what PostgreSQL does. */
  @Test
  void jdbc() throws Exception {
    try (Connection connection = quirks.getJdbcConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate("DROP TABLE IF EXISTS adbcpkeytest");
      statement.executeUpdate("CREATE TABLE adbcpkeytest (key SERIAL PRIMARY KEY, value INT)");
      statement.executeUpdate("INSERT INTO adbcpkeytest (value) VALUES (0), (1), (2)");
      statement.executeUpdate("ANALYZE adbcpkeytest");

      int count = 0;
      try (ResultSet rs =
          connection.getMetaData().getIndexInfo(null, null, "adbcpkeytest", false, true)) {
        ResultSetMetaData rsmd = rs.getMetaData();
        while (rs.next()) {
          // For debugging
          for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            System.out.println(rsmd.getColumnName(i) + " => " + rs.getObject(i));
          }
          System.out.println("===");

          // TABLE_NAME
          assertThat(rs.getString(3)).isEqualTo("adbcpkeytest");
          // TYPE
          assertThat(rs.getShort(7)).isEqualTo(DatabaseMetaData.tableIndexOther);
          // CARDINALITY
          assertThat(rs.getLong(11)).isEqualTo(3);

          count++;
        }
      }

      assertThat(count).isEqualTo(1);
    }
  }
}
