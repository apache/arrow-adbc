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
import org.apache.arrow.adbc.core.IngestOption;
import org.apache.arrow.adbc.core.IsolationLevel;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.adbc.driver.jni.impl.JniLoader;
import org.apache.arrow.adbc.driver.jni.impl.NativeConnectionHandle;
import org.apache.arrow.adbc.driver.jni.impl.NativeStatementHandle;
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
  public void cancel() throws AdbcException {
    JniLoader.INSTANCE.connectionCancel(handle);
  }

  @Override
  public AdbcStatement bulkIngest(String targetTableName, BulkIngestMode mode)
      throws AdbcException {
    return bulkIngestImpl(targetTableName, mode);
  }

  @Override
  public AdbcStatement bulkIngest(
      String targetTableName, BulkIngestMode mode, IngestOption... options) throws AdbcException {
    return bulkIngestImpl(targetTableName, mode, options);
  }

  AdbcStatement bulkIngestImpl(String targetTableName, BulkIngestMode mode, IngestOption... options)
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

      JniLoader.INSTANCE.statementSetOptionString(
          stmtHandle, "adbc.ingest.target_table", targetTableName);
      JniLoader.INSTANCE.statementSetOptionString(stmtHandle, "adbc.ingest.mode", modeValue);

      for (var option : options) {
        if (option instanceof IngestOption.TemporaryIngestOption) {
          var o = (IngestOption.TemporaryIngestOption) option;
          JniLoader.INSTANCE.statementSetOptionString(
              stmtHandle, "adbc.ingest.temporary", Boolean.toString(o.isTemporary()));
        } else if (option instanceof IngestOption.TargetNamespaceIngestOption) {
          var o = (IngestOption.TargetNamespaceIngestOption) option;
          if (o.getTargetCatalog() != null) {
            JniLoader.INSTANCE.statementSetOptionString(
                stmtHandle, "adbc.ingest.target_catalog", o.getTargetCatalog());
          }
          if (o.getTargetDbSchema() != null) {
            JniLoader.INSTANCE.statementSetOptionString(
                stmtHandle, "adbc.ingest.target_db_schema", o.getTargetDbSchema());
          }
        }
      }

      return new JniStatement(allocator, stmtHandle);
    } catch (Exception e) {
      stmtHandle.close();
      throw e;
    }
  }

  @Override
  public ArrowReader getInfo(int @Nullable [] infoCodes) throws AdbcException {
    return JniLoader.INSTANCE.connectionGetInfo(handle, infoCodes).importStream(allocator);
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
    return JniLoader.INSTANCE
        .connectionGetObjects(
            handle,
            depth.ordinal(),
            catalogPattern,
            dbSchemaPattern,
            tableNamePattern,
            tableTypes,
            columnNamePattern)
        .importStream(allocator);
  }

  @Override
  public Schema getTableSchema(String catalog, String dbSchema, String tableName)
      throws AdbcException {
    return JniLoader.INSTANCE
        .connectionGetTableSchema(handle, catalog, dbSchema, tableName)
        .importSchema(allocator);
  }

  @Override
  public ArrowReader getTableTypes() throws AdbcException {
    return JniLoader.INSTANCE.connectionGetTableTypes(handle).importStream(allocator);
  }

  @Override
  public boolean getReadOnly() throws AdbcException {
    return getOption(JniDriver.READONLY);
  }

  @Override
  public void setReadOnly(boolean isReadOnly) throws AdbcException {
    setOption(JniDriver.READONLY, isReadOnly);
  }

  @Override
  public boolean getAutoCommit() throws AdbcException {
    return getOption(JniDriver.AUTOCOMMIT);
  }

  @Override
  public void setAutoCommit(boolean enableAutoCommit) throws AdbcException {
    setOption(JniDriver.AUTOCOMMIT, enableAutoCommit);
  }

  @Override
  public IsolationLevel getIsolationLevel() throws AdbcException {
    String level = getOption(JniDriver.ISOLATION_LEVEL);
    if (level == null) {
      return null;
    }
    switch (level) {
      case JniDriver.ISOLATION_LEVEL_READ_UNCOMMITTED:
        return IsolationLevel.READ_UNCOMMITTED;
      case JniDriver.ISOLATION_LEVEL_READ_COMMITTED:
        return IsolationLevel.READ_COMMITTED;
      case JniDriver.ISOLATION_LEVEL_REPEATABLE_READ:
        return IsolationLevel.REPEATABLE_READ;
      case JniDriver.ISOLATION_LEVEL_SNAPSHOT:
        return IsolationLevel.SNAPSHOT;
      case JniDriver.ISOLATION_LEVEL_SERIALIZABLE:
        return IsolationLevel.SERIALIZABLE;
      default:
        throw AdbcException.invalidArgument("[jni] invalid isolation level value: " + level);
    }
  }

  @Override
  public void setIsolationLevel(IsolationLevel level) throws AdbcException {
    if (level == null) {
      setOption(JniDriver.ISOLATION_LEVEL, (String) null);
    } else {
      String levelValue;
      switch (level) {
        case READ_UNCOMMITTED:
          levelValue = JniDriver.ISOLATION_LEVEL_READ_UNCOMMITTED;
          break;
        case READ_COMMITTED:
          levelValue = JniDriver.ISOLATION_LEVEL_READ_COMMITTED;
          break;
        case REPEATABLE_READ:
          levelValue = JniDriver.ISOLATION_LEVEL_REPEATABLE_READ;
          break;
        case SNAPSHOT:
          levelValue = JniDriver.ISOLATION_LEVEL_SNAPSHOT;
          break;
        case SERIALIZABLE:
          levelValue = JniDriver.ISOLATION_LEVEL_SERIALIZABLE;
          break;
        default:
          throw new IllegalArgumentException("Unknown isolation level: " + level);
      }
      setOption(JniDriver.ISOLATION_LEVEL, levelValue);
    }
  }

  @Override
  public void commit() throws AdbcException {
    JniLoader.INSTANCE.connectionCommit(handle);
  }

  @Override
  public void rollback() throws AdbcException {
    JniLoader.INSTANCE.connectionRollback(handle);
  }

  @Override
  public String getCurrentCatalog() throws AdbcException {
    return getOption(JniDriver.CURRENT_CATALOG);
  }

  @Override
  public String getCurrentDbSchema() throws AdbcException {
    return getOption(JniDriver.CURRENT_DB_SCHEMA);
  }

  @Override
  public void setCurrentCatalog(String catalog) throws AdbcException {
    setOption(JniDriver.CURRENT_CATALOG, catalog);
  }

  @Override
  public void setCurrentDbSchema(String dbSchema) throws AdbcException {
    setOption(JniDriver.CURRENT_DB_SCHEMA, dbSchema);
  }

  @Override
  public void close() {
    handle.close();
  }

  @Override
  public <T> T getOption(TypedKey<T> key) throws AdbcException {
    if (key.getType() == String.class) {
      return key.cast(JniLoader.INSTANCE.connectionGetOptionString(handle, key.getKey()));
    } else if (key.getType() == Integer.class) {
      return key.cast((int) JniLoader.INSTANCE.connectionGetOptionLong(handle, key.getKey()));
    } else if (key.getType() == Long.class) {
      return key.cast(JniLoader.INSTANCE.connectionGetOptionLong(handle, key.getKey()));
    } else if (key.getType() == Float.class) {
      return key.cast((float) JniLoader.INSTANCE.connectionGetOptionDouble(handle, key.getKey()));
    } else if (key.getType() == Double.class) {
      return key.cast(JniLoader.INSTANCE.connectionGetOptionDouble(handle, key.getKey()));
    } else if (key.getType() == Boolean.class) {
      String value = JniLoader.INSTANCE.connectionGetOptionString(handle, key.getKey());
      if (value == null) {
        return null;
      } else if ("true".equalsIgnoreCase(value)) {
        return key.cast(Boolean.TRUE);
      } else if ("false".equalsIgnoreCase(value)) {
        return key.cast(Boolean.FALSE);
      } else {
        throw AdbcException.invalidArgument(
            "[jni] invalid boolean value for option " + key.getKey() + ": " + value);
      }
    } else if (key.getType() == byte[].class) {
      return key.cast(JniLoader.INSTANCE.connectionGetOptionBytes(handle, key.getKey()));
    }
    return AdbcConnection.super.getOption(key);
  }

  @Override
  public <T> void setOption(TypedKey<T> key, T value) throws AdbcException {
    if (value instanceof String) {
      JniLoader.INSTANCE.connectionSetOptionString(handle, key.getKey(), (String) value);
    } else if (value == null) {
      JniLoader.INSTANCE.connectionSetOptionString(handle, key.getKey(), null);
    } else if (value instanceof Integer) {
      JniLoader.INSTANCE.connectionSetOptionLong(handle, key.getKey(), (Integer) value);
    } else if (value instanceof Long) {
      JniLoader.INSTANCE.connectionSetOptionLong(handle, key.getKey(), (Long) value);
    } else if (value instanceof Float) {
      JniLoader.INSTANCE.connectionSetOptionDouble(handle, key.getKey(), (Float) value);
    } else if (value instanceof Double) {
      JniLoader.INSTANCE.connectionSetOptionDouble(handle, key.getKey(), (Double) value);
    } else if (value instanceof Boolean) {
      JniLoader.INSTANCE.connectionSetOptionString(
          handle, key.getKey(), ((Boolean) value) ? "true" : "false");
    } else if (value instanceof byte[]) {
      JniLoader.INSTANCE.connectionSetOptionBytes(handle, key.getKey(), (byte[]) value);
    } else {
      throw AdbcException.invalidArgument(
          "[jni] unsupported connection option type " + value.getClass());
    }
  }
}
