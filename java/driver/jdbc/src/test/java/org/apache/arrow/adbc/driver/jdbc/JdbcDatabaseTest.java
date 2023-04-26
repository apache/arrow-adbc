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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("resource") // due to driver.open calls
class JdbcDatabaseTest {
  BufferAllocator allocator;
  AdbcDriver driver;

  @BeforeEach
  void beforeEach() {
    allocator = new RootAllocator();
    driver = new JdbcDriver(allocator);
  }

  @AfterEach
  void afterEach() throws Exception {
    AutoCloseables.close(allocator);
  }

  @Test
  void conflictingUriOption() {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(JdbcDriver.PARAM_URI, "jdbc:derby:memory:newUriOption;create=true");
    parameters.put(AdbcDriver.PARAM_URL, "jdbc:derby:memory:newUriOption;create=true");
    assertThat(assertThrows(AdbcException.class, () -> driver.open(parameters)))
        .hasMessageContaining("Provide at most one of [uri, adbc.url]");
  }

  @Test
  void conflictingUriDataSourceOption() {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(JdbcDriver.PARAM_URI, "jdbc:derby:memory:newUriOption;create=true");
    parameters.put(
        JdbcDriver.PARAM_DATASOURCE,
        new UrlDataSource("jdbc:derby:memory:newUriOption;create=true"));
    assertThat(assertThrows(AdbcException.class, () -> driver.open(parameters)))
        .hasMessageContaining("Provide at most one of uri and adbc.jdbc.datasource");
  }

  @Test
  void noUri() {
    Map<String, Object> parameters = new HashMap<>();
    assertThat(assertThrows(AdbcException.class, () -> driver.open(parameters)))
        .hasMessageContaining("Must provide one of uri and adbc.jdbc.datasource options");
  }

  @Test
  void badUriOptionType() {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(JdbcDriver.PARAM_URI, 5);
    assertThat(assertThrows(AdbcException.class, () -> driver.open(parameters)))
        .hasMessageContaining(
            "[uri, adbc.url] must be a class java.lang.String, not a class java.lang.Integer");
  }

  @Test
  void badSqlQuirksOptionType() {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(JdbcDriver.PARAM_URI, "");
    parameters.put(JdbcDriver.PARAM_SQL_QUIRKS, "");
    assertThat(assertThrows(AdbcException.class, () -> driver.open(parameters)))
        .hasMessageContaining(
            "[adbc.sql.quirks] must be a class org.apache.arrow.adbc.sql.SqlQuirks, not a class"
                + " java.lang.String");
  }

  @Test
  void badUsernameOptionType() {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(JdbcDriver.PARAM_URI, "");
    parameters.put("username", 2);
    assertThat(assertThrows(AdbcException.class, () -> driver.open(parameters)))
        .hasMessageContaining(
            "[username] must be a class java.lang.String, not a class java.lang.Integer");

    parameters.clear();
    parameters.put(JdbcDriver.PARAM_URI, "");
    parameters.put("password", 2);
    assertThat(assertThrows(AdbcException.class, () -> driver.open(parameters)))
        .hasMessageContaining(
            "[password] must be a class java.lang.String, not a class java.lang.Integer");
  }

  @Test
  void usernameWithoutPassword() {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(JdbcDriver.PARAM_URI, "");
    parameters.put("username", "");
    assertThat(assertThrows(AdbcException.class, () -> driver.open(parameters)))
        .hasMessageContaining("Must provide both or neither of username and password");

    parameters.clear();
    parameters.put(JdbcDriver.PARAM_URI, "");
    parameters.put("password", "");
    assertThat(assertThrows(AdbcException.class, () -> driver.open(parameters)))
        .hasMessageContaining("Must provide both or neither of username and password");
  }
}
