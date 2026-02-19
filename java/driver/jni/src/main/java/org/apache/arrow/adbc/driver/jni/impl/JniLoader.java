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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

/** Singleton wrapper protecting access to JNI functions. */
public enum JniLoader {
  INSTANCE;

  JniLoader() {
    // The JAR may contain multiple binaries for different platforms, so load the appropriate one.
    final String libraryName = "adbc_driver_jni";
    String libraryToLoad =
        libraryName + "/" + getNormalizedArch() + "/" + System.mapLibraryName(libraryName);

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

  private String getNormalizedArch() {
    // Be consistent with our CMake config
    String arch = System.getProperty("os.arch").toLowerCase(Locale.US);
    switch (arch) {
      case "amd64":
        return "x86_64";
      case "aarch64":
        return "aarch_64";
      default:
        throw new RuntimeException("ADBC JNI driver not supported on architecture " + arch);
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

  public void statementSetOption(NativeStatementHandle statement, String key, String value)
      throws AdbcException {
    NativeAdbc.statementSetOption(statement.getStatementHandle(), key, value);
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
}
