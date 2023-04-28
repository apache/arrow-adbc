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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.memory.BufferAllocator;

/** Account for driver/vendor-specific quirks in implementing validation tests. */
public abstract class SqlValidationQuirks {
  public abstract AdbcDatabase initDatabase(BufferAllocator allocator) throws AdbcException;

  public void cleanupTable(String name) throws Exception {}

  /** Get the name of the default catalog. */
  public abstract String defaultCatalog();

  /** Normalize a table name. */
  public String caseFoldTableName(String name) {
    return name;
  }

  /** Normalize a column name. */
  public String caseFoldColumnName(String name) {
    return name;
  }

  /** Generates a query to set a column to NOT NULL in a table. */
  public String generateSetNotNullQuery(String table, String column) {
    return "ALTER TABLE "
        + caseFoldTableName(table)
        + " ALTER COLUMN "
        + caseFoldColumnName(column)
        + " SET NOT NULL";
  }

  public String generateAddPrimaryKeyQuery(
      String constraintName, String table, List<String> columns) {
    return "ALTER TABLE "
        + caseFoldTableName(table)
        + " \n"
        + "  ADD CONSTRAINT "
        + constraintName
        + " \n"
        + "  PRIMARY KEY ("
        + columns.stream().map(this::caseFoldColumnName).collect(Collectors.joining(","))
        + ")";
  }

  public String generateAddForeignKeyQuery(
      String constraintName,
      String table,
      String column,
      String referenceTable,
      String referenceColumn) {
    return "ALTER TABLE "
        + caseFoldTableName(table)
        + " \n"
        + "  ADD CONSTRAINT "
        + constraintName
        + " \n"
        + "  FOREIGN KEY ("
        + caseFoldColumnName(column)
        + ") \n"
        + "  REFERENCES "
        + caseFoldTableName(referenceTable)
        + " ("
        + caseFoldColumnName(referenceColumn)
        + ") ";
  }
}
