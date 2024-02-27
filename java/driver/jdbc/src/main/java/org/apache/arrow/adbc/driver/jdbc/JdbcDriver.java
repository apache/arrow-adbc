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
package org.apache.arrow.adbc.driver.jdbc;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.memory.BufferAllocator;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An ADBC driver wrapping the JDBC API. */
public class JdbcDriver implements AdbcDriver {
  /** A parameter for creating an {@link AdbcDatabase} from a {@link DataSource}. */
  public static final String PARAM_DATASOURCE = "adbc.jdbc.datasource";

  /** A parameter for specifying backend-specific configuration (type: {@link JdbcQuirks}). */
  public static final String PARAM_JDBC_QUIRKS = "adbc.jdbc.quirks";

  /**
   * A parameter for specifying a URI to connect to.
   *
   * <p>Matches the parameter used by C and Go.
   */
  public static final String PARAM_URI = "uri";

  private final BufferAllocator allocator;

  public JdbcDriver(BufferAllocator allocator) {
    this.allocator = Objects.requireNonNull(allocator);
  }

  @Override
  public AdbcDatabase open(Map<String, Object> parameters) throws AdbcException {
    DataSource dataSource = getParam(DataSource.class, parameters, PARAM_DATASOURCE);
    // XXX(apache/arrow-adbc#316): allow "uri" to align with C/Go
    String target = getParam(String.class, parameters, PARAM_URI, PARAM_URL);
    if (dataSource != null && target != null) {
      throw AdbcException.invalidArgument(
          "[JDBC] Provide at most one of " + PARAM_URI + " and " + PARAM_DATASOURCE);
    }

    SqlQuirks quirks = getParam(SqlQuirks.class, parameters, PARAM_SQL_QUIRKS);
    JdbcQuirks jdbcQuirks = getParam(JdbcQuirks.class, parameters, PARAM_JDBC_QUIRKS);
    if (jdbcQuirks != null && quirks != null) {
      throw AdbcException.invalidArgument(
          "[JDBC] Provide at most one of " + PARAM_SQL_QUIRKS + " and " + PARAM_JDBC_QUIRKS);
    } else if (jdbcQuirks == null) {
      if (quirks == null) {
        quirks = new SqlQuirks();
      }
      jdbcQuirks = JdbcQuirks.builder("unknown").sqlQuirks(quirks).build();
    }

    String username = getParam(String.class, parameters, "username");
    String password = getParam(String.class, parameters, "password");
    if ((username != null && password == null) || (username == null && password != null)) {
      throw AdbcException.invalidArgument(
          "[JDBC] Must provide both or neither of username and password");
    }

    if (target != null) {
      dataSource = new UrlDataSource(target);
    }

    if (dataSource == null) {
      throw AdbcException.invalidArgument(
          "[JDBC] Must provide one of " + PARAM_URI + " and " + PARAM_DATASOURCE + " options");
    }
    return new JdbcDataSourceDatabase(allocator, dataSource, username, password, jdbcQuirks);
  }

  private static <T> @Nullable T getParam(
      Class<T> klass, Map<String, Object> parameters, String... choices) throws AdbcException {
    Object result = null;
    for (String choice : choices) {
      Object value = parameters.get(choice);
      if (value != null) {
        if (result != null) {
          throw AdbcException.invalidArgument(
              "[JDBC] Provide at most one of " + Arrays.toString(choices));
        }
        result = value;
      }
    }
    if (result == null) {
      return null;
    }

    try {
      return klass.cast(result);
    } catch (ClassCastException e) {
      throw AdbcException.invalidArgument(
          "[JDBC] "
              + Arrays.toString(choices)
              + " must be a "
              + klass
              + ", not a "
              + result.getClass());
    }
  }
}
