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

package org.apache.arrow.adbc.driver.jdbc.adapter;

import java.sql.Types;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;

public final class JdbcToArrowTypeConverters {
  public static final JdbcToArrowTypeConverter MICROSOFT_SQL_SERVER =
      JdbcToArrowTypeConverters::mssql;
  public static final JdbcToArrowTypeConverter POSTGRESQL = JdbcToArrowTypeConverters::postgresql;
  private static final int MS_SQL_TYPE_DATETIMEOFFSET = -155;

  private static ArrowType mssql(JdbcFieldInfoExtra field) {
    switch (field.getJdbcType()) {
      case Types.TIME:
        return MinorType.TIMENANO.getType();
      case Types.TIMESTAMP:
        // DATETIME2
        // Precision is "100 nanoseconds" -> TimeUnit is NANOSECOND
        return MinorType.TIMESTAMPNANO.getType();
      case MS_SQL_TYPE_DATETIMEOFFSET:
        // DATETIMEOFFSET
        // Precision is "100 nanoseconds" -> TimeUnit is NANOSECOND
        return new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC");
      default:
        return JdbcToArrowUtils.getArrowTypeFromJdbcType(field.getFieldInfo(), /*calendar*/ null);
    }
  }

  private static ArrowType postgresql(JdbcFieldInfoExtra field) {
    switch (field.getJdbcType()) {
      case Types.TIME:
        return MinorType.TIMEMICRO.getType();
      case Types.TIMESTAMP:
        {
          int decimalDigits = field.getScale();
          final TimeUnit unit;
          if (decimalDigits == 0) {
            unit = TimeUnit.SECOND;
          } else if (decimalDigits > 0 && decimalDigits <= 3) {
            unit = TimeUnit.MILLISECOND;
          } else if (decimalDigits > 0 && decimalDigits <= 6) {
            unit = TimeUnit.MICROSECOND;
          } else if (decimalDigits > 6) {
            unit = TimeUnit.NANOSECOND;
          } else {
            // Negative precision?
            throw new UnsupportedOperationException(
                "Cannot convert type to Arrow Timestamp (precision is negative)");
          }
          if ("timestamptz".equals(field.getTypeName())) {
            return new ArrowType.Timestamp(unit, "UTC");
          } else if ("timestamp".equals(field.getTypeName())) {
            return new ArrowType.Timestamp(unit, /*timezone*/ null);
          }
          // Unknown type
          throw new UnsupportedOperationException("Cannot convert type to Arrow Timestamp");
        }
      default:
        return JdbcToArrowUtils.getArrowTypeFromJdbcType(field.getFieldInfo(), /*calendar*/ null);
    }
  }
}
