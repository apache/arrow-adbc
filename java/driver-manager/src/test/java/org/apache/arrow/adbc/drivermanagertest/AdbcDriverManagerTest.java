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

package org.apache.arrow.adbc.drivermanagertest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Function;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;
import org.apache.arrow.adbc.test.TestDriver;
import org.apache.arrow.adbc.test.TestDriverFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

/** Test cases for {@link AdbcDriverManager}. */
public class AdbcDriverManagerTest {
  @Test
  public void testDriverFromServiceLoader() {
    final Function<BufferAllocator, AdbcDriver> driverFactoryFunction =
        AdbcDriverManager.getInstance().lookupDriver(TestDriverFactory.class.getCanonicalName());

    assertThat(driverFactoryFunction).isNotNull();

    try (BufferAllocator allocator = new RootAllocator()) {
      final AdbcDriver driverInstance = driverFactoryFunction.apply(allocator);
      assertThat(driverInstance.getClass()).isEqualTo(TestDriver.class);
    }
  }
}
