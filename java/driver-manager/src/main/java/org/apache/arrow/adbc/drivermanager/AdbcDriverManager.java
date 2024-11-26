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
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Instantiate connections to ADBC databases generically based on driver name. */
public final class AdbcDriverManager {
  private static final AdbcDriverManager INSTANCE = new AdbcDriverManager();

  private final ConcurrentMap<String, Function<BufferAllocator, AdbcDriver>> driverFactoryFunctions;

  private AdbcDriverManager() {
    driverFactoryFunctions = new ConcurrentHashMap<>();
    final ServiceLoader<AdbcDriverFactory> serviceLoader =
        ServiceLoader.load(AdbcDriverFactory.class);
    for (AdbcDriverFactory driverFactory : serviceLoader) {
      final @Nullable String className = driverFactory.getClass().getCanonicalName();
      if (className == null) {
        throw new RuntimeException("Class has no canonical name");
      }
      driverFactoryFunctions.putIfAbsent(className, driverFactory::getDriver);
    }
  }

  /**
   * Connect to a database.
   *
   * @param driverFactoryName The driver to use.
   * @param allocator The allocator to use.
   * @param parameters Parameters for the driver.
   * @return The AdbcDatabase instance.
   * @throws AdbcException if the driver was not found or if connection fails.
   */
  public AdbcDatabase connect(
      String driverFactoryName, BufferAllocator allocator, Map<String, Object> parameters)
      throws AdbcException {
    final Function<BufferAllocator, AdbcDriver> driverFactoryFunction =
        lookupDriver(driverFactoryName);
    if (driverFactoryFunction == null) {
      throw new AdbcException(
          "AdbcDriverFactory not found for '" + driverFactoryName + "'",
          null,
          AdbcStatusCode.NOT_FOUND,
          null,
          0);
    }

    return driverFactoryFunction.apply(allocator).open(parameters);
  }

  /**
   * Lookup a registered driver.
   *
   * @param driverFactoryName The driver factory function to lookup. This is usually the
   *     fully-qualified class name of an AdbcDriverFactory class.
   * @return A function to construct an AdbcDriver from a BufferAllocator, or null if not found.
   */
  @Nullable Function<BufferAllocator, AdbcDriver> lookupDriver(String driverFactoryName) {
    return driverFactoryFunctions.get(driverFactoryName);
  }

  /**
   * Register a driver manually.
   *
   * @param driverFactoryName The name of the driver to associate with the construction function.
   * @param driverFactory The function to use to instantiate the driver.
   */
  public void registerDriver(
      String driverFactoryName, Function<BufferAllocator, AdbcDriver> driverFactory) {
    if (driverFactoryFunctions.putIfAbsent(driverFactoryName, driverFactory) != null) {
      throw new IllegalStateException(
          "[DriverManager] Driver factory already registered for '" + driverFactoryName + "'");
    }
  }

  public static AdbcDriverManager getInstance() {
    return INSTANCE;
  }
}
