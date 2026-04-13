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

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.driver.testsuite.SqlTestUtil;
import org.apache.arrow.adbc.driver.testsuite.SqlValidationQuirks;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.TimeUnit;
import org.junit.jupiter.api.Assumptions;

public class JniDb2Quirks extends SqlValidationQuirks {
  static final String DB2_URI_ENV_VAR = "ADBC_DB2_TEST_URI";

  static String getUri() {
    final String uri = System.getenv(DB2_URI_ENV_VAR);
    if (SqlTestUtil.isCI() && (uri == null || uri.isEmpty())) {
      throw new RuntimeException("DB2 not found, set " + DB2_URI_ENV_VAR);
    }
    Assumptions.assumeFalse(uri == null, "DB2 not found, set " + DB2_URI_ENV_VAR);
    Assumptions.assumeFalse(uri.isEmpty(), "DB2 not found, set " + DB2_URI_ENV_VAR);
    return uri;
  }

  @Override
  public AdbcDatabase initDatabase(BufferAllocator allocator) throws AdbcException {
    final Map<String, Object> parameters = new HashMap<>();
    JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_db2");
    AdbcDriver.PARAM_URI.set(parameters, getUri());
    return new JniDriver(allocator).open(parameters);
  }

  @Override
  public String defaultCatalog() {
    return "";
  }

  @Override
  public String caseFoldTableName(String name) {
    return name.toUpperCase();
  }

  @Override
  public String caseFoldColumnName(String name) {
    return name.toUpperCase();
  }

  @Override
  public TimeUnit defaultTimeUnit() {
    return TimeUnit.MICROSECOND;
  }

  @Override
  public TimeUnit defaultTimestampUnit() {
    return TimeUnit.MICROSECOND;
  }

  @Override
  public boolean supportsCurrentCatalog() {
    return false;
  }
}
