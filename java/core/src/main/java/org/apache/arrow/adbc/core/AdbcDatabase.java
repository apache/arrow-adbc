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

/**
 * An instance of a database (e.g. a handle to an in-memory database).
 *
 * <p>A database can have multiple open connections. While this is likely redundant structure for
 * remote/networked databases, for in-memory databases, this object provides an explicit point of
 * ownership.
 */
public interface AdbcDatabase extends AdbcCloseable, AdbcOptions {
  /** Create a new connection to the database. */
  AdbcConnection connect() throws AdbcException;
}
