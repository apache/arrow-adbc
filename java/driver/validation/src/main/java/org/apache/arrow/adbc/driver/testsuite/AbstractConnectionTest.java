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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.util.AutoCloseables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class AbstractConnectionTest {
  /** Must be initialized by the subclass. */
  protected static SqlValidationQuirks quirks;

  protected AdbcDatabase database;
  protected AdbcConnection connection;

  @BeforeEach
  public void beforeEach() throws Exception {
    database = quirks.initDatabase();
    connection = database.connect();
  }

  @AfterEach
  public void afterEach() throws Exception {
    AutoCloseables.close(connection, database);
  }

  @Test
  void multipleConnections() throws Exception {
    try (final AdbcConnection ignored = database.connect()) {}
  }

  @Test
  void readOnly() throws Exception {
    assertThat(connection.getReadOnly()).isFalse();
  }

  @Test
  void isolationLevel() throws Exception {
    connection.getIsolationLevel();
  }
}
