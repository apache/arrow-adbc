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
  /** Create the target table; fail if already exists. */
  BulkIngestBuilder createMode() throws AdbcException;

  /** Append to the target table; fail if not found. */
  BulkIngestBuilder appendMode() throws AdbcException;

  /** Drop (if exists) and create the target table. */
  BulkIngestBuilder replaceMode() throws AdbcException;

  /** Create the target table if necessary, append if already exists. */
  BulkIngestBuilder createAppendMode() throws AdbcException;

  /**
   * How to handle an already-existing/non-existent target table. Default is {@link
   * BulkIngestMode#CREATE}.
   *
   * <p>Also see the convenience methods {@link #createMode()}, {@link #appendMode()}, {@link
   * #replaceMode()}, and {@link #createAppendMode()}.
   */
  BulkIngestBuilder mode(BulkIngestMode mode) throws AdbcException;

  /**
   * Ingest data from the given root.
   *
   * <p>Like {@link AdbcStatement#bind(VectorSchemaRoot)}, this ingest will NOT close the given
   * root. If a different root or stream is bound, this root will simply be forgotten.
   */
  BulkIngestBuilder bind(VectorSchemaRoot root) throws AdbcException;

  /**
   * Ingest data from the given reader.
   *
   * <p>Like {@link AdbcStatement#bind(ArrowReader)}, this ingest WILL close the given stream. If a
   * different root or stream is bound, this stream will be closed.
   */
  BulkIngestBuilder bind(ArrowReader stream) throws AdbcException;

  /** Ingest into a table with the given name (required). */
  BulkIngestBuilder targetTable(String table) throws AdbcException;

  /** Ingest into a table in the given schema. */
  BulkIngestBuilder targetSchema(String schema) throws AdbcException;

  /** Ingest into a table in the given catalog. */
  BulkIngestBuilder targetCatalog(String catalog) throws AdbcException;

  /** Ingest into a temporary table. */
  default BulkIngestBuilder temporary() throws AdbcException {
    return this.temporary(true);
  }

  /** Whether to ingest into a temporary table or not. */
  BulkIngestBuilder temporary(boolean isTemporary) throws AdbcException;

  /** Set an arbitrary ingest option. */
  BulkIngestBuilder option(IngestOption option) throws AdbcException;

  /** Set a statement option before ingest. */
  <T> BulkIngestBuilder option(TypedKey<T> key, T value) throws AdbcException;

  /**
   * Convert this builder to an {@link AdbcStatement} (low-level API for further control) without
   * executing.
   *
   * <p>The caller is responsible for closing the returned statement.
   */
  AdbcStatement toStatement() throws AdbcException;

  /**
   * Execute the bulk ingest.
   *
   * <p>This will consume the bound stream, if any. To ingest again, bind a new stream or root or
   * update data in the already-bound root.
   */
  default AdbcStatement.UpdateResult ingest() throws AdbcException {
    try (var statement = toStatement()) {
      return statement.executeUpdate();
    }
  }

  /** Clean up any state in this bulk ingest. */
  @Override
  void close() throws AdbcException;
}
