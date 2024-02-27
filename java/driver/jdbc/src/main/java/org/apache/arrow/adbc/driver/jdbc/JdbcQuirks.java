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

import java.util.Objects;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.adbc.driver.jdbc.adapter.JdbcToArrowTypeConverter;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Backend-specific quirks for the JDBC adapter. */
public class JdbcQuirks {
  final String backendName;
  JdbcToArrowTypeConverter typeConverter;
  @Nullable SqlQuirks sqlQuirks;

  public JdbcQuirks(String backendName) {
    this.backendName = Objects.requireNonNull(backendName);
    this.typeConverter =
        (fieldInfo) ->
            JdbcToArrowUtils.getArrowTypeFromJdbcType(fieldInfo.getFieldInfo(), /*calendar*/ null);
  }

  /** The name of the backend that these quirks are targeting. */
  public String getBackendName() {
    return backendName;
  }

  /** The conversion from JDBC type to Arrow type. */
  public JdbcToArrowTypeConverter getTypeConverter() {
    return typeConverter;
  }

  /** The SQL syntax quirks. */
  public @Nullable SqlQuirks getSqlQuirks() {
    return sqlQuirks;
  }

  /** Create a new builder. */
  public static JdbcQuirks.Builder builder(final String backendName) {
    return new JdbcQuirks.Builder(backendName);
  }

  @Override
  public String toString() {
    return "JdbcQuirks{" + backendName + '}';
  }

  public static final class Builder {
    final JdbcQuirks quirks;

    public Builder(String backendName) {
      quirks = new JdbcQuirks(backendName);
    }

    public Builder typeConverter(JdbcToArrowTypeConverter typeConverter) {
      quirks.typeConverter = Objects.requireNonNull(typeConverter);
      return this;
    }

    public Builder sqlQuirks(SqlQuirks sqlQuirks) {
      quirks.sqlQuirks = sqlQuirks;
      return this;
    }

    public JdbcQuirks build() {
      return quirks;
    }
  }
}
