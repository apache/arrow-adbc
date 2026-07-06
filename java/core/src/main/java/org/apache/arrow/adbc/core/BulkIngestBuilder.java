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
import org.apache.arrow.vector.ipc.ArrowReader;

/** A fluent builder-style interface for configuring and executing a bulk ingest. */
public interface BulkIngestBuilder extends AutoCloseable {
  BulkIngestBuilder create() throws AdbcException;

  BulkIngestBuilder append() throws AdbcException;

  BulkIngestBuilder replace() throws AdbcException;

  BulkIngestBuilder createAppend() throws AdbcException;

  BulkIngestBuilder mode(BulkIngestMode mode) throws AdbcException;

  BulkIngestBuilder bind(VectorSchemaRoot root) throws AdbcException;

  BulkIngestBuilder bind(ArrowReader stream) throws AdbcException;

  BulkIngestBuilder targetTable(String table) throws AdbcException;

  BulkIngestBuilder targetSchema(String schema) throws AdbcException;

  BulkIngestBuilder targetCatalog(String catalog) throws AdbcException;

  default BulkIngestBuilder temporary() throws AdbcException {
    return this.temporary(true);
  }

  BulkIngestBuilder temporary(boolean isTemporary) throws AdbcException;

  BulkIngestBuilder option(IngestOption option) throws AdbcException;

  <T> BulkIngestBuilder option(TypedKey<T> key, T value) throws AdbcException;

  AdbcStatement toStatement() throws AdbcException;

  default AdbcStatement.UpdateResult ingest() throws AdbcException {
    try (var statement = toStatement()) {
      return statement.executeUpdate();
    }
  }

  @Override
  void close() throws AdbcException;
}
