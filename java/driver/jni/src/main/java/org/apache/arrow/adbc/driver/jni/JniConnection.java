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

package org.apache.arrow.adbc.driver.jni;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.driver.jni.impl.JniLoader;
import org.apache.arrow.adbc.driver.jni.impl.NativeConnectionHandle;
import org.apache.arrow.adbc.driver.jni.impl.NativeQueryResult;
import org.apache.arrow.adbc.driver.jni.impl.NativeStatementHandle;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

public class JniConnection implements AdbcConnection {
  private final BufferAllocator allocator;
  private final NativeConnectionHandle handle;

  public JniConnection(BufferAllocator allocator, NativeConnectionHandle handle) {
    this.allocator = allocator;
    this.handle = handle;
  }

  @Override
  public AdbcStatement createStatement() throws AdbcException {
    return new JniStatement(allocator, JniLoader.INSTANCE.openStatement(handle));
  }

  @Override
  public AdbcStatement bulkIngest(String targetTableName, BulkIngestMode mode)
      throws AdbcException {
    NativeStatementHandle stmtHandle = JniLoader.INSTANCE.openStatement(handle);
    try {
      String modeValue;
      switch (mode) {
        case CREATE:
          modeValue = "adbc.ingest.mode.create";
          break;
        case APPEND:
          modeValue = "adbc.ingest.mode.append";
          break;
        case REPLACE:
          modeValue = "adbc.ingest.mode.replace";
          break;
        case CREATE_APPEND:
          modeValue = "adbc.ingest.mode.create_append";
          break;
        default:
          throw new IllegalArgumentException("Unknown bulk ingest mode: " + mode);
      }

      JniLoader.INSTANCE.statementSetOption(
          stmtHandle, "adbc.ingest.target_table", targetTableName);
      JniLoader.INSTANCE.statementSetOption(stmtHandle, "adbc.ingest.mode", modeValue);

      return new JniStatement(allocator, stmtHandle);
    } catch (Exception e) {
      stmtHandle.close();
      throw e;
    }
  }

  private ArrowReader importStream(NativeQueryResult result) {
    try (final ArrowArrayStream cStream = ArrowArrayStream.wrap(result.cDataStream())) {
      return Data.importArrayStream(allocator, cStream);
    }
  }

  @Override
  public ArrowReader getInfo(int @Nullable [] infoCodes) throws AdbcException {
    NativeQueryResult result = JniLoader.INSTANCE.connectionGetInfo(handle, infoCodes);
    return importStream(result);
  }

  @Override
  public ArrowReader getObjects(
      GetObjectsDepth depth,
      String catalogPattern,
      String dbSchemaPattern,
      String tableNamePattern,
      String[] tableTypes,
      String columnNamePattern)
      throws AdbcException {
    NativeQueryResult result =
        JniLoader.INSTANCE.connectionGetObjects(
            handle,
            depth.ordinal(),
            catalogPattern,
            dbSchemaPattern,
            tableNamePattern,
            tableTypes,
            columnNamePattern);
    return importStream(result);
  }

  @Override
  public Schema getTableSchema(String catalog, String dbSchema, String tableName)
      throws AdbcException {
    long schemaAddress =
        JniLoader.INSTANCE.connectionGetTableSchema(handle, catalog, dbSchema, tableName);
    try (final ArrowSchema cSchema = ArrowSchema.wrap(schemaAddress);
        final CDataDictionaryProvider provider = new CDataDictionaryProvider()) {
      return Data.importSchema(allocator, cSchema, provider);
    }
  }

  @Override
  public ArrowReader getTableTypes() throws AdbcException {
    NativeQueryResult result = JniLoader.INSTANCE.connectionGetTableTypes(handle);
    return importStream(result);
  }

  @Override
  public void close() {
    handle.close();
  }
}
