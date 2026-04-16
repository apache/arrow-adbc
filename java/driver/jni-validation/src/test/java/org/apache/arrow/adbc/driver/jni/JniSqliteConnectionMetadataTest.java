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

import org.apache.arrow.adbc.driver.testsuite.AbstractConnectionMetadataTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;

public class JniSqliteConnectionMetadataTest extends AbstractConnectionMetadataTest {
  @BeforeAll
  static void beforeAll() {
    quirks = new JniSqliteQuirks();
  }

  @Override
  @Disabled("SQLite getInfo schema differs: map value type is List<Int32> instead of Int32")
  public void getInfo() {}

  @Override
  @Disabled("SQLite getInfo schema differs: map value type is List<Int32> instead of Int32")
  public void getInfoByCode() {}

  @Override
  @Disabled("SQLite does not support ALTER TABLE ... ALTER COLUMN ... SET NOT NULL")
  public void getObjectsConstraints() {}

  @Override
  @Disabled(
      "SQLite getObjects schema has nullable catalog_name/db_schema_name, test expects non-null")
  public void getObjectsColumns() {}

  @Override
  @Disabled(
      "SQLite getObjects schema has nullable catalog_name/db_schema_name, test expects non-null")
  public void getObjectsCatalogs() {}

  @Override
  @Disabled(
      "SQLite getObjects schema has nullable catalog_name/db_schema_name, test expects non-null")
  public void getObjectsCatalogsPattern() {}

  @Override
  @Disabled(
      "SQLite getObjects schema has nullable catalog_name/db_schema_name, test expects non-null")
  public void getObjectsDbSchemas() {}

  @Override
  @Disabled(
      "SQLite getObjects schema has nullable catalog_name/db_schema_name, test expects non-null")
  public void getObjectsTables() {}

  @Override
  @Disabled("SQLite type affinity returns Int64 for all types, causing schema mismatch")
  public void getTableSchema() {}
}
