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
package org.apache.arrow.adbc.driver.jdbc.derby;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.adbc.driver.testsuite.AbstractConnectionTest;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DerbyConnectionTest extends AbstractConnectionTest {
  @TempDir static Path tempDir;

  @BeforeAll
  static void beforeAll() {
    quirks = new DerbyQuirks(tempDir);
  }

  @Test
  void newUrlOption() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      AdbcDriver driver = new JdbcDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      parameters.put(JdbcDriver.PARAM_URI, "jdbc:derby:memory:newUrlOption;create=true");
      try (final AdbcDatabase database = driver.open(parameters)) {
        assertThat(database).isNotNull();
      }
    }
  }

  @Test
  void dataSourceOption() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      EmbeddedDataSource dataSource = new EmbeddedDataSource();
      dataSource.setDatabaseName("memory:dataSourceOption");
      dataSource.setCreateDatabase("create");
      AdbcDriver driver = new JdbcDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      parameters.put(JdbcDriver.PARAM_DATASOURCE, dataSource);
      try (final AdbcDatabase database = driver.open(parameters)) {
        assertThat(database).isNotNull();
      }
    }
  }
}
