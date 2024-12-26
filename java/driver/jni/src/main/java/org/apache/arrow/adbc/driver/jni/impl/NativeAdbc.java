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

public class NativeAdbc {
  public static native NativeHandle openDatabase(int version, String[] parameters)
      throws NativeAdbcException;

  public static native void closeDatabase(long handle) throws NativeAdbcException;

  public static native NativeHandle openConnection(long databaseHandle) throws NativeAdbcException;

  public static native void closeConnection(long handle) throws NativeAdbcException;

  public static native NativeHandle openStatement(long connectionHandle) throws NativeAdbcException;

  public static native void closeStatement(long handle) throws NativeAdbcException;

  public static native NativeQueryResult statementExecuteQuery(long handle)
      throws NativeAdbcException;

  public static native void statementSetSqlQuery(long handle, String query)
      throws NativeAdbcException;
}
