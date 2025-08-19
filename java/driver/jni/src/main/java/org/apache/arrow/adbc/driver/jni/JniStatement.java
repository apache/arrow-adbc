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

import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.jni.impl.JniLoader;
import org.apache.arrow.adbc.driver.jni.impl.NativeQueryResult;
import org.apache.arrow.adbc.driver.jni.impl.NativeStatementHandle;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

public class JniStatement implements AdbcStatement {
  private final BufferAllocator allocator;
  private final NativeStatementHandle handle;

  public JniStatement(BufferAllocator allocator, NativeStatementHandle handle) {
    this.allocator = allocator;
    this.handle = handle;
  }

  @Override
  public void setSqlQuery(String query) throws AdbcException {
    JniLoader.INSTANCE.statementSetSqlQuery(handle, query);
  }

  @Override
  public QueryResult executeQuery() throws AdbcException {
    NativeQueryResult result = JniLoader.INSTANCE.statementExecuteQuery(handle);
    // TODO: need to handle result in such a way that we free it even if we error here
    ArrowReader reader;
    try (final ArrowArrayStream cStream = ArrowArrayStream.wrap(result.cDataStream())) {
      reader = org.apache.arrow.c.Data.importArrayStream(allocator, cStream);
    }
    return new QueryResult(result.rowsAffected(), reader);
  }

  @Override
  public UpdateResult executeUpdate() throws AdbcException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void prepare() throws AdbcException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    handle.close();
  }
}
