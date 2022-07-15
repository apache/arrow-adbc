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

import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;

/** Account for driver/vendor-specific quirks in implementing validation tests. */
public abstract class SqlValidationQuirks {
  public abstract AdbcDatabase initDatabase() throws AdbcException;

  public void cleanupTable(String name) throws Exception {}

  /** Normalize a table name. */
  public String caseFoldTableName(String name) {
    return name;
  }

  /** Normalize a column name. */
  public String caseFoldColumnName(String name) {
    return name;
  }
}
