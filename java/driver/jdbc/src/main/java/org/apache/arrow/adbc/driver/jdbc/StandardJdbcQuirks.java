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

import org.apache.arrow.adbc.driver.jdbc.adapter.JdbcToArrowTypeConverters;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.vector.types.pojo.ArrowType;

public final class StandardJdbcQuirks {
  public static final JdbcQuirks MS_SQL_SERVER =
      JdbcQuirks.builder("Microsoft SQL Server")
          .typeConverter(JdbcToArrowTypeConverters.MICROSOFT_SQL_SERVER)
          .build();
  public static final JdbcQuirks POSTGRESQL =
      JdbcQuirks.builder("PostgreSQL")
          .sqlQuirks(
              SqlQuirks.builder()
                  .arrowToSqlTypeNameMapping(
                      (arrowType -> {
                        if (arrowType.getTypeID() == ArrowType.ArrowTypeID.Utf8) {
                          return "TEXT";
                        }
                        return SqlQuirks.DEFAULT_ARROW_TYPE_TO_SQL_TYPE_NAME_MAPPING.apply(
                            arrowType);
                      }))
                  .build())
          .typeConverter(JdbcToArrowTypeConverters.POSTGRESQL)
          .build();
}
