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
package org.apache.arrow.adbc.core;

import org.apache.arrow.vector.VectorSchemaRoot;

/** A connection to a {@link AdbcDatabase}. */
public interface AdbcConnection extends AutoCloseable {
  /**
   * Commit the pending transaction.
   *
   * @throws AdbcException if a database error occurs
   * @throws IllegalStateException if autocommit is enabled
   * @throws UnsupportedOperationException if the database does not support transactions
   */
  default void commit() throws AdbcException {
    throw new UnsupportedOperationException("Connection does not support transactions");
  }

  /** Create a new statement that can be executed. */
  AdbcStatement createStatement() throws AdbcException;

  /**
   * Create a new statement to bulk insert a {@link VectorSchemaRoot} into a table.
   *
   * <p>Bind data to the statement, then call {@link AdbcStatement#execute()}. The table will be
   * created if it does not exist. Otherwise data will be appended. <tt>execute()</tt> will throw
   * AdbcException with status {@link AdbcStatusCode#ALREADY_EXISTS} if the schema of the bound data
   * does not match the table schema.
   */
  default AdbcStatement bulkIngest(String targetTableName) throws AdbcException {
    throw new UnsupportedOperationException("Connection does not support bulk ingestion");
  }

  /**
   * Rollback the pending transaction.
   *
   * @throws AdbcException if a database error occurs
   * @throws IllegalStateException if autocommit is enabled
   * @throws UnsupportedOperationException if the database does not support transactions
   */
  default void rollback() throws AdbcException {
    throw new UnsupportedOperationException("Connection does not support transactions");
  }

  /**
   * Get the autocommit state.
   *
   * <p>Connections start in autocommit mode by default.
   */
  default boolean getAutoCommit() throws AdbcException {
    return true;
  }

  /**
   * Toggle whether autocommit is enabled.
   *
   * @throws UnsupportedOperationException if the database does not support toggling autocommit
   */
  default void setAutoCommit(boolean enableAutoCommit) throws AdbcException {
    throw new UnsupportedOperationException("Connection does not support transactions");
  }
}
