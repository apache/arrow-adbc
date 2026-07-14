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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.checkerframework.checker.nullness.qual.Nullable;

class BulkIngestBuilderImpl implements BulkIngestBuilder {
  private final AdbcConnection connection;
  BulkIngestMode mode = BulkIngestMode.CREATE;
  @Nullable VectorSchemaRoot root;
  @Nullable ArrowReader reader;
  @Nullable String targetTable, targetSchema, targetCatalog;
  boolean temporary = false;
  List<IngestOption> ingestOptions = new ArrayList<>();
  List<Map.Entry<TypedKey<?>, Object>> options = new ArrayList<>();

  BulkIngestBuilderImpl(AdbcConnection connection) {
    this.connection = Objects.requireNonNull(connection, "connection must not be null");
  }

  @Override
  public BulkIngestBuilder createMode() {
    mode = BulkIngestMode.CREATE;
    return this;
  }

  @Override
  public BulkIngestBuilder appendMode() {
    mode = BulkIngestMode.APPEND;
    return this;
  }

  @Override
  public BulkIngestBuilder replaceMode() {
    mode = BulkIngestMode.REPLACE;
    return this;
  }

  @Override
  public BulkIngestBuilder createAppendMode() {
    mode = BulkIngestMode.CREATE_APPEND;
    return this;
  }

  @Override
  public BulkIngestBuilder mode(BulkIngestMode mode) {
    this.mode = mode;
    return this;
  }

  @Override
  public BulkIngestBuilder bind(VectorSchemaRoot root) throws AdbcException {
    this.root = root;
    if (this.reader != null) {
      try {
        this.reader.close();
      } catch (IOException e) {
        throw AdbcException.io("failed to close existing ArrowReader").withCause(e);
      }
    }
    this.reader = null;
    return this;
  }

  @Override
  public BulkIngestBuilder bind(ArrowReader stream) throws AdbcException {
    root = null;
    reader = stream;
    return this;
  }

  @Override
  public BulkIngestBuilder targetTable(String table) throws AdbcException {
    targetTable = table;
    return this;
  }

  @Override
  public BulkIngestBuilder targetSchema(String schema) throws AdbcException {
    targetSchema = schema;
    return this;
  }

  @Override
  public BulkIngestBuilder targetCatalog(String catalog) throws AdbcException {
    targetCatalog = catalog;
    return this;
  }

  @Override
  public BulkIngestBuilder temporary(boolean isTemporary) throws AdbcException {
    temporary = isTemporary;
    return this;
  }

  @Override
  public BulkIngestBuilder option(IngestOption option) throws AdbcException {
    ingestOptions.add(option);
    return this;
  }

  @Override
  public <T> BulkIngestBuilder option(TypedKey<T> key, T value) throws AdbcException {
    // redundant cast - make ErrorProne happy
    options.add(Map.entry(key, Objects.requireNonNull((Object) value)));
    return this;
  }

  @Override
  public AdbcStatement toStatement() throws AdbcException {
    AdbcStatement statement = null;
    try {
      if (targetTable == null) {
        throw AdbcException.invalidArgument("must set targetTable");
      }

      List<IngestOption> ingestOptions = new ArrayList<>(this.ingestOptions);
      if (temporary) {
        ingestOptions.add(IngestOption.TEMPORARY);
      }
      if (targetCatalog != null) {
        ingestOptions.add(IngestOption.targetCatalog(targetCatalog));
      }
      if (targetSchema != null) {
        ingestOptions.add(IngestOption.targetDbSchema(targetSchema));
      }
      IngestOption[] varargs = ingestOptions.toArray(new IngestOption[0]);
      statement = connection.bulkIngest(Objects.requireNonNull(targetTable), mode, varargs);

      if (root != null) {
        statement.bind(root);
        root = null;
      } else if (reader != null) {
        statement.bind(reader);
        // Prevent reader from being closed below; it's now owned by the statement
        reader = null;
      } else {
        throw AdbcException.invalidArgument(
            "must bind either a VectorSchemaRoot or an ArrowReader");
      }

      for (Map.Entry<TypedKey<?>, Object> option : options) {
        //noinspection unchecked
        statement.setOption((TypedKey<Object>) option.getKey(), option.getValue());
      }
    } catch (Exception e) {
      if (statement != null) {
        statement.close();
      }
      throw e;
    }
    return statement;
  }

  @Override
  public void close() throws AdbcException {
    // Do not close root
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        throw AdbcException.io("failed to close ArrowReader").withCause(e);
      }
    }
  }
}
