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

package org.apache.arrow.adbc.drivermanager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Load an ADBC driver based on name.
 *
 * <p>This class is EXPERIMENTAL. It will be rewritten before 1.0.0 (see <a
 * href="https://github.com/apache/arrow-adbc/issues/48">issue #48</a>).
 */
public final class AdbcDriverManager {
  private static final AdbcDriverManager INSTANCE = new AdbcDriverManager();

  private final ConcurrentMap<String, Function<BufferAllocator, AdbcDriver>> drivers;

  public AdbcDriverManager() {
    drivers = new ConcurrentHashMap<>();
  }

  /**
   * Connect to a database.
   *
   * @param driverName The driver to use.
   * @param allocator The allocator to use.
   * @param parameters Parameters for the driver.
   * @return The AdbcDatabase instance.
   * @throws AdbcException if the driver was not found or if connection fails.
   */
  public AdbcDatabase connect(
      String driverName, BufferAllocator allocator, Map<String, Object> parameters)
      throws AdbcException {
    final Function<BufferAllocator, AdbcDriver> driver = lookupDriver(driverName);
    if (driver == null) {
      throw new AdbcException(
          "Driver not found for '" + driverName + "'", null, AdbcStatusCode.NOT_FOUND, null, 0);
    }
    return driver.apply(allocator).open(parameters);
  }

  /**
   * Lookup a registered driver.
   *
   * @param driverName The driver to lookup.
   * @return The driver instance, or null if not found.
   */
  public Function<BufferAllocator, AdbcDriver> lookupDriver(String driverName) {
    return drivers.get(driverName);
  }

  public void registerDriver(String driverName, Function<BufferAllocator, AdbcDriver> driver) {
    if (drivers.putIfAbsent(driverName, driver) != null) {
      throw new IllegalStateException(
          "[DriverManager] Driver already registered for '" + driverName + "'");
    }
  }

  public static AdbcDriverManager getInstance() {
    return INSTANCE;
  }
}
