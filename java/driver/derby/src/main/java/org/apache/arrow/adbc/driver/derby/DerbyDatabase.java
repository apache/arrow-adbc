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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.memory.BufferAllocator;

/** An instance of a database (e.g. a handle to an in-memory database). */
public final class DerbyDatabase implements AdbcDatabase {
  private final BufferAllocator allocator;
  private final String target;
  private final Connection connection;

  DerbyDatabase(BufferAllocator allocator, final String target) throws AdbcException {
    try {
      new org.apache.derby.jdbc.EmbeddedDriver();

      this.allocator = allocator;
      this.target = target;
      this.connection = DriverManager.getConnection(target + ";create=true");
    } catch (SQLException e) {
      throw new AdbcException(e);
    }
  }

  @Override
  public AdbcConnection connect() throws AdbcException {
    final Connection connection;
    try {
      connection = DriverManager.getConnection(target);
    } catch (SQLException e) {
      throw new AdbcException(e);
    }
    // TODO: better naming of child allocator
    return new DerbyConnection(
        allocator.newChildAllocator("derby-connection", 0, allocator.getLimit()), connection);
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

  @Override
  public String toString() {
    return "DerbyDatabase{" + "target='" + target + '\'' + '}';
  }
}
