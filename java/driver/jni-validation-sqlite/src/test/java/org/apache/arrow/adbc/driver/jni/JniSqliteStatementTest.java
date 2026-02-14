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

import org.apache.arrow.adbc.driver.testsuite.AbstractStatementTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;

class JniSqliteStatementTest extends AbstractStatementTest {
  @BeforeAll
  static void beforeAll() {
    quirks = new JniSqliteQuirks();
  }

  @Override
  @Disabled("SQLite driver does not return expected ADBC status codes for schema mismatch")
  public void bulkIngestAppendConflict() {}

  @Override
  @Disabled("SQLite driver returns INTERNAL instead of NOT_FOUND")
  public void bulkIngestAppendNotFound() {}

  @Override
  @Disabled("SQLite driver returns INTERNAL instead of ALREADY_EXISTS")
  public void bulkIngestCreateConflict() {}

  @Override
  @Disabled("Not yet implemented in JNI driver")
  public void executeSchema() {}

  @Override
  @Disabled("Not yet implemented in JNI driver")
  public void executeSchemaPrepared() {}

  @Override
  @Disabled("Not yet implemented in JNI driver")
  public void executeSchemaParams() {}

  @Override
  @Disabled("Not yet implemented in JNI driver")
  public void getParameterSchema() {}

  @Override
  @Disabled("SQLite promotes INT to BIGINT, causing strict schema equality check to fail")
  public void prepareQueryWithParameters() {}
}
