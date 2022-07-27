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

package org.apache.arrow.adbc.sql;

import java.util.function.Function;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** Parameters to pass to SQL-based drivers to account for driver/vendor-specific SQL quirks. */
public final class SqlQuirks {
  public static final Function<ArrowType, String> DEFAULT_ARROW_TYPE_TO_SQL_TYPE_NAME_MAPPING =
      (arrowType) -> {
        switch (arrowType.getTypeID()) {
          case Null:
          case Struct:
          case List:
          case LargeList:
          case FixedSizeList:
          case Union:
          case Map:
            return null;
          case Int:
            // TODO:
            return "INT";
          case FloatingPoint:
            return null;
          case Utf8:
            return "CLOB";
          case LargeUtf8:
          case Binary:
          case LargeBinary:
          case FixedSizeBinary:
          case Bool:
          case Decimal:
          case Date:
          case Time:
          case Timestamp:
          case Interval:
          case Duration:
          case NONE:
          default:
            return null;
        }
      };
  Function<ArrowType, String> arrowToSqlTypeNameMapping;

  public SqlQuirks() {
    this.arrowToSqlTypeNameMapping = DEFAULT_ARROW_TYPE_TO_SQL_TYPE_NAME_MAPPING;
  }

  /** The mapping from Arrow type to SQL type name, used to build queries. */
  public Function<ArrowType, String> getArrowToSqlTypeNameMapping() {
    return arrowToSqlTypeNameMapping;
  }

  /** Create a new builder. */
  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    Function<ArrowType, String> arrowToSqlTypeNameMapping;

    public Builder arrowToSqlTypeNameMapping(Function<ArrowType, String> mapper) {
      this.arrowToSqlTypeNameMapping = mapper;
      return this;
    }

    public SqlQuirks build() {
      final SqlQuirks quirks = new SqlQuirks();
      if (arrowToSqlTypeNameMapping != null) {
        quirks.arrowToSqlTypeNameMapping = arrowToSqlTypeNameMapping;
      }
      return quirks;
    }
  }
}
