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

import java.nio.ByteBuffer;
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

  static native void statementBind(long handle, long values, long schema) throws AdbcException;

  // TODO(lidavidm): we need a way to bind an ArrowReader (or some other suitable interface that
  // doesn't exist in arrow-java; see the discussion around the Avro reader about how ArrowReader
  // isn't a very general interface)
  @SuppressWarnings("unused")
  static native void statementBindStream(long handle, long stream) throws AdbcException;

  static native void statementCancel(long handle) throws AdbcException;

  static native long statementExecuteUpdate(long handle) throws AdbcException;

  static native void statementPrepare(long handle) throws AdbcException;

  static native NativePartitionResult statementExecutePartitions(long handle) throws AdbcException;

  static native NativeQueryResult statementExecuteQuery(long handle) throws AdbcException;

  static native NativeSchemaResult statementExecuteSchema(long handle) throws AdbcException;

  static native NativeSchemaResult statementGetParameterSchema(long statementHandle)
      throws AdbcException;

  static native void statementSetSqlQuery(long handle, String query) throws AdbcException;

  static native byte[] statementGetOptionBytes(long handle, String key) throws AdbcException;

  static native double statementGetOptionDouble(long handle, String key) throws AdbcException;

  static native long statementGetOptionLong(long handle, String key) throws AdbcException;

  static native String statementGetOptionString(long handle, String key) throws AdbcException;

  static native void statementSetOptionBytes(long handle, String key, byte[] value)
      throws AdbcException;

  static native void statementSetOptionDouble(long handle, String key, double value)
      throws AdbcException;

  static native void statementSetOptionLong(long handle, String key, long value)
      throws AdbcException;

  static native void statementSetOptionString(long handle, String key, String value)
      throws AdbcException;

  static native void connectionCancel(long handle) throws AdbcException;

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

  static native NativeSchemaResult connectionGetTableSchema(
      long handle, String catalog, String dbSchema, String tableName) throws AdbcException;

  static native NativeQueryResult connectionGetTableTypes(long handle) throws AdbcException;

  static native void connectionCommit(long handle) throws AdbcException;

  static native void connectionRollback(long handle) throws AdbcException;

  static native NativeQueryResult connectionReadPartition(long handle, ByteBuffer partition)
      throws AdbcException;

  static native byte[] connectionGetOptionBytes(long handle, String key) throws AdbcException;

  static native double connectionGetOptionDouble(long handle, String key) throws AdbcException;

  static native long connectionGetOptionLong(long handle, String key) throws AdbcException;

  static native String connectionGetOptionString(long handle, String key) throws AdbcException;

  static native void connectionSetOptionBytes(long handle, String key, byte[] value)
      throws AdbcException;

  static native void connectionSetOptionDouble(long handle, String key, double value)
      throws AdbcException;

  static native void connectionSetOptionLong(long handle, String key, long value)
      throws AdbcException;

  static native void connectionSetOptionString(long handle, String key, String value)
      throws AdbcException;

  static native byte[] databaseGetOptionBytes(long handle, String key) throws AdbcException;

  static native double databaseGetOptionDouble(long handle, String key) throws AdbcException;

  static native long databaseGetOptionLong(long handle, String key) throws AdbcException;

  static native String databaseGetOptionString(long handle, String key) throws AdbcException;

  static native void databaseSetOptionBytes(long handle, String key, byte[] value)
      throws AdbcException;

  static native void databaseSetOptionDouble(long handle, String key, double value)
      throws AdbcException;

  static native void databaseSetOptionLong(long handle, String key, long value)
      throws AdbcException;

  static native void databaseSetOptionString(long handle, String key, String value)
      throws AdbcException;
}
