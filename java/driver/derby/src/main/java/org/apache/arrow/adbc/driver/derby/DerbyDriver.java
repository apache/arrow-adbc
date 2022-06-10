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
package org.apache.arrow.adbc.driver.derby;

import java.util.Map;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public enum DerbyDriver implements AdbcDriver {
  INSTANCE;

  private final BufferAllocator allocator;

  DerbyDriver() {
    allocator = new RootAllocator();
    AdbcDriverManager.getInstance().registerDriver("org.apache.arrow.adbc.driver.derby", this);
  }

  @Override
  public AdbcDatabase connect(Map<String, String> parameters) throws AdbcException {
    return new DerbyDatabase(allocator, "jdbc:derby:" + parameters.get("path"));
  }
}
