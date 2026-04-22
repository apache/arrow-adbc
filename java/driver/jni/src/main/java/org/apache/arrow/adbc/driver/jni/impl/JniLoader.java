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

package org.apache.arrow.adbc.driver.jni.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

/** Singleton wrapper protecting access to JNI functions. */
public enum JniLoader {
  INSTANCE;

  JniLoader() {
    // If 'arrow.adbc.driver.jni.library.path' is set, load from there instead of the JAR.
    String resolvedPath = JniLibraryResolver.resolve();
    if (resolvedPath != null) {
      System.load(resolvedPath);
      return;
    }

    // The JAR may contain multiple binaries for different platforms, so load the appropriate one.
    String libraryToLoad = JniLibraryResolver.resourcePath();

    try {
      InputStream is = JniLoader.class.getClassLoader().getResourceAsStream(libraryToLoad);
      if (is == null) {
        throw new FileNotFoundException(
            "No JNI library for current platform, missing from JAR: " + libraryToLoad);
      }
      File temp =
          File.createTempFile("adbc-jni-", ".tmp", new File(System.getProperty("java.io.tmpdir")));
      temp.deleteOnExit();

      try (is) {
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
      Runtime.getRuntime().load(temp.getAbsolutePath());
    } catch (IOException e) {
      throw new IllegalStateException("Error loading native library " + libraryToLoad, e);
    }
  }

  public NativeDatabaseHandle openDatabase(Map<String, String> parameters) throws AdbcException {
    String[] nativeParameters = new String[parameters.size() * 2];
    int index = 0;
    for (Map.Entry<String, String> parameter : parameters.entrySet()) {
      nativeParameters[index++] = parameter.getKey();
      nativeParameters[index++] = parameter.getValue();
    }
    return NativeAdbc.openDatabase(1001000, nativeParameters);
  }

  public NativeConnectionHandle openConnection(NativeDatabaseHandle database) throws AdbcException {
    return NativeAdbc.openConnection(database.getDatabaseHandle());
  }

  public NativeStatementHandle openStatement(NativeConnectionHandle connection)
      throws AdbcException {
    return NativeAdbc.openStatement(connection.getConnectionHandle());
  }

  public void statementCancel(NativeStatementHandle statement) throws AdbcException {
    NativeAdbc.statementCancel(statement.getStatementHandle());
  }

  public NativePartitionResult statementExecutePartitions(NativeStatementHandle statement)
      throws AdbcException {
    return NativeAdbc.statementExecutePartitions(statement.getStatementHandle());
  }

  public NativeQueryResult statementExecuteQuery(NativeStatementHandle statement)
      throws AdbcException {
    return NativeAdbc.statementExecuteQuery(statement.getStatementHandle());
  }

  public void statementSetSqlQuery(NativeStatementHandle statement, String query)
      throws AdbcException {
    NativeAdbc.statementSetSqlQuery(statement.getStatementHandle(), query);
  }

  public void statementBind(NativeStatementHandle statement, ArrowArray batch, ArrowSchema schema)
      throws AdbcException {
    NativeAdbc.statementBind(
        statement.getStatementHandle(), batch.memoryAddress(), schema.memoryAddress());
  }

  public long statementExecuteUpdate(NativeStatementHandle statement) throws AdbcException {
    return NativeAdbc.statementExecuteUpdate(statement.getStatementHandle());
  }

  public void statementPrepare(NativeStatementHandle statement) throws AdbcException {
    NativeAdbc.statementPrepare(statement.getStatementHandle());
  }

  public NativeSchemaResult statementExecuteSchema(NativeStatementHandle statement)
      throws AdbcException {
    return NativeAdbc.statementExecuteSchema(statement.getStatementHandle());
  }

  public NativeSchemaResult statementGetParameterSchema(NativeStatementHandle statement)
      throws AdbcException {
    return NativeAdbc.statementGetParameterSchema(statement.getStatementHandle());
  }

  public byte[] statementGetOptionBytes(NativeStatementHandle handle, String key)
      throws AdbcException {
    return NativeAdbc.statementGetOptionBytes(handle.getStatementHandle(), key);
  }

  public double statementGetOptionDouble(NativeStatementHandle handle, String key)
      throws AdbcException {
    return NativeAdbc.statementGetOptionDouble(handle.getStatementHandle(), key);
  }

  public long statementGetOptionLong(NativeStatementHandle handle, String key)
      throws AdbcException {
    return NativeAdbc.statementGetOptionLong(handle.getStatementHandle(), key);
  }

  public String statementGetOptionString(NativeStatementHandle handle, String key)
      throws AdbcException {
    return NativeAdbc.statementGetOptionString(handle.getStatementHandle(), key);
  }

  public void statementSetOptionBytes(NativeStatementHandle handle, String key, byte[] value)
      throws AdbcException {
    NativeAdbc.statementSetOptionBytes(handle.getStatementHandle(), key, value);
  }

  public void statementSetOptionDouble(NativeStatementHandle handle, String key, double value)
      throws AdbcException {
    NativeAdbc.statementSetOptionDouble(handle.getStatementHandle(), key, value);
  }

  public void statementSetOptionLong(NativeStatementHandle handle, String key, long value)
      throws AdbcException {
    NativeAdbc.statementSetOptionLong(handle.getStatementHandle(), key, value);
  }

  public void statementSetOptionString(NativeStatementHandle statement, String key, String value)
      throws AdbcException {
    NativeAdbc.statementSetOptionString(statement.getStatementHandle(), key, value);
  }

  public void connectionCancel(NativeConnectionHandle connection) throws AdbcException {
    NativeAdbc.connectionCancel(connection.getConnectionHandle());
  }

  public NativeQueryResult connectionGetObjects(
      NativeConnectionHandle connection,
      int depth,
      String catalog,
      String dbSchema,
      String tableName,
      String[] tableTypes,
      String columnName)
      throws AdbcException {
    return NativeAdbc.connectionGetObjects(
        connection.getConnectionHandle(),
        depth,
        catalog,
        dbSchema,
        tableName,
        tableTypes,
        columnName);
  }

  public NativeQueryResult connectionGetInfo(NativeConnectionHandle connection, int[] infoCodes)
      throws AdbcException {
    return NativeAdbc.connectionGetInfo(connection.getConnectionHandle(), infoCodes);
  }

  public NativeSchemaResult connectionGetTableSchema(
      NativeConnectionHandle connection, String catalog, String dbSchema, String tableName)
      throws AdbcException {
    return NativeAdbc.connectionGetTableSchema(
        connection.getConnectionHandle(), catalog, dbSchema, tableName);
  }

  public NativeQueryResult connectionGetTableTypes(NativeConnectionHandle connection)
      throws AdbcException {
    return NativeAdbc.connectionGetTableTypes(connection.getConnectionHandle());
  }

  public void connectionCommit(NativeConnectionHandle connection) throws AdbcException {
    NativeAdbc.connectionCommit(connection.getConnectionHandle());
  }

  public void connectionRollback(NativeConnectionHandle connection) throws AdbcException {
    NativeAdbc.connectionRollback(connection.getConnectionHandle());
  }

  public NativeQueryResult connectionReadPartition(
      NativeConnectionHandle connection, ByteBuffer partition) throws AdbcException {
    return NativeAdbc.connectionReadPartition(connection.getConnectionHandle(), partition);
  }

  public byte[] connectionGetOptionBytes(NativeConnectionHandle handle, String key)
      throws AdbcException {
    return NativeAdbc.connectionGetOptionBytes(handle.getConnectionHandle(), key);
  }

  public double connectionGetOptionDouble(NativeConnectionHandle handle, String key)
      throws AdbcException {
    return NativeAdbc.connectionGetOptionDouble(handle.getConnectionHandle(), key);
  }

  public long connectionGetOptionLong(NativeConnectionHandle handle, String key)
      throws AdbcException {
    return NativeAdbc.connectionGetOptionLong(handle.getConnectionHandle(), key);
  }

  public String connectionGetOptionString(NativeConnectionHandle handle, String key)
      throws AdbcException {
    return NativeAdbc.connectionGetOptionString(handle.getConnectionHandle(), key);
  }

  public void connectionSetOptionBytes(NativeConnectionHandle handle, String key, byte[] value)
      throws AdbcException {
    NativeAdbc.connectionSetOptionBytes(handle.getConnectionHandle(), key, value);
  }

  public void connectionSetOptionDouble(NativeConnectionHandle handle, String key, double value)
      throws AdbcException {
    NativeAdbc.connectionSetOptionDouble(handle.getConnectionHandle(), key, value);
  }

  public void connectionSetOptionLong(NativeConnectionHandle handle, String key, long value)
      throws AdbcException {
    NativeAdbc.connectionSetOptionLong(handle.getConnectionHandle(), key, value);
  }

  public void connectionSetOptionString(NativeConnectionHandle connection, String key, String value)
      throws AdbcException {
    NativeAdbc.connectionSetOptionString(connection.getConnectionHandle(), key, value);
  }

  public byte[] databaseGetOptionBytes(NativeDatabaseHandle handle, String key)
      throws AdbcException {
    return NativeAdbc.databaseGetOptionBytes(handle.getDatabaseHandle(), key);
  }

  public double databaseGetOptionDouble(NativeDatabaseHandle handle, String key)
      throws AdbcException {
    return NativeAdbc.databaseGetOptionDouble(handle.getDatabaseHandle(), key);
  }

  public long databaseGetOptionLong(NativeDatabaseHandle handle, String key) throws AdbcException {
    return NativeAdbc.databaseGetOptionLong(handle.getDatabaseHandle(), key);
  }

  public String databaseGetOptionString(NativeDatabaseHandle handle, String key)
      throws AdbcException {
    return NativeAdbc.databaseGetOptionString(handle.getDatabaseHandle(), key);
  }

  public void databaseSetOptionBytes(NativeDatabaseHandle handle, String key, byte[] value)
      throws AdbcException {
    NativeAdbc.databaseSetOptionBytes(handle.getDatabaseHandle(), key, value);
  }

  public void databaseSetOptionDouble(NativeDatabaseHandle handle, String key, double value)
      throws AdbcException {
    NativeAdbc.databaseSetOptionDouble(handle.getDatabaseHandle(), key, value);
  }

  public void databaseSetOptionLong(NativeDatabaseHandle handle, String key, long value)
      throws AdbcException {
    NativeAdbc.databaseSetOptionLong(handle.getDatabaseHandle(), key, value);
  }

  public void databaseSetOptionString(NativeDatabaseHandle handle, String key, String value)
      throws AdbcException {
    NativeAdbc.databaseSetOptionString(handle.getDatabaseHandle(), key, value);
  }
}
