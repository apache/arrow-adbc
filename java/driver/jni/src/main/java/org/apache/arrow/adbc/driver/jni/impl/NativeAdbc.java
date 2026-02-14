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

import org.apache.arrow.adbc.core.AdbcException;

/** All the JNI methods. Don't use this directly, prefer {@link JniLoader}. */
class NativeAdbc {
  static native NativeDatabaseHandle openDatabase(int version, String[] parameters)
      throws AdbcException;

  static native void closeDatabase(long handle) throws AdbcException;

  static native NativeConnectionHandle openConnection(long databaseHandle) throws AdbcException;

  static native void closeConnection(long handle) throws AdbcException;

  static native NativeStatementHandle openStatement(long connectionHandle) throws AdbcException;

  static native void closeStatement(long handle) throws AdbcException;

  static native NativeQueryResult statementExecuteQuery(long handle) throws AdbcException;

  static native void statementSetSqlQuery(long handle, String query) throws AdbcException;

  static native void statementBind(long handle, long values, long schema) throws AdbcException;

  // TODO(lidavidm): we need a way to bind an ArrowReader (or some other suitable interface that
  // doesn't exist in arrow-java; see the discussion around the Avro reader about how ArrowReader
  // isn't a very general interface)
  @SuppressWarnings("unused")
  static native void statementBindStream(long handle, long stream) throws AdbcException;

  static native long statementExecuteUpdate(long handle) throws AdbcException;

  static native void statementPrepare(long handle) throws AdbcException;

  static native void statementSetOption(long handle, String key, String value) throws AdbcException;

  static native NativeQueryResult connectionGetObjects(
      long handle,
      int depth,
      String catalog,
      String dbSchema,
      String tableName,
      String[] tableTypes,
      String columnName)
      throws AdbcException;

  static native NativeQueryResult connectionGetInfo(long handle, int[] infoCodes)
      throws AdbcException;

  static native long connectionGetTableSchema(
      long handle, String catalog, String dbSchema, String tableName) throws AdbcException;

  static native NativeQueryResult connectionGetTableTypes(long handle) throws AdbcException;
}
