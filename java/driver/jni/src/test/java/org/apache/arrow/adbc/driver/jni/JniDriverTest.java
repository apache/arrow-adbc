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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

class JniDriverTest {
  @Test
  void minimal() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");

      driver.open(parameters).close();
    }
  }

  @Test
  void query() throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      JniDriver driver = new JniDriver(allocator);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");

      try (final AdbcDatabase db = driver.open(parameters);
          final AdbcConnection conn = db.connect();
          final AdbcStatement stmt = conn.createStatement()) {
        stmt.setSqlQuery("SELECT 1");
        try (final AdbcStatement.QueryResult result = stmt.executeQuery()) {
          assertThat(result.getReader().loadNextBatch()).isTrue();
          assertThat(result.getReader().getVectorSchemaRoot().getVector(0).getObject(0))
              .isEqualTo(1L);
        }
      }
    }
  }
}
