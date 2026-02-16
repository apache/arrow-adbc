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
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class JniStatement implements AdbcStatement {
  private final BufferAllocator allocator;
  private final NativeStatementHandle handle;
  private VectorSchemaRoot bindRoot;

  public JniStatement(BufferAllocator allocator, NativeStatementHandle handle) {
    this.allocator = allocator;
    this.handle = handle;
  }

  @Override
  public void setSqlQuery(String query) throws AdbcException {
    JniLoader.INSTANCE.statementSetSqlQuery(handle, query);
  }

  @Override
  public void bind(VectorSchemaRoot root) throws AdbcException {
    this.bindRoot = root;
  }

  // The C Data export takes ownership of the data at bind time and ignores subsequent
  // client changes to the bound root. Defer the export until execution so we capture
  // the final state of the VectorSchemaRoot.
  private void exportBind() throws AdbcException {
    if (bindRoot == null) {
      return;
    }
    try (final ArrowArray batch = ArrowArray.allocateNew(allocator);
        final ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      // TODO(lidavidm): we may need a way to separately provide a dictionary provider
      Data.exportSchema(allocator, bindRoot.getSchema(), null, schema);
      Data.exportVectorSchemaRoot(allocator, bindRoot, null, batch);

      JniLoader.INSTANCE.statementBind(handle, batch, schema);
    }
  }

  @Override
  public QueryResult executeQuery() throws AdbcException {
    exportBind();
    NativeQueryResult result = JniLoader.INSTANCE.statementExecuteQuery(handle);
    return new QueryResult(result.rowsAffected(), result.importStream(allocator));
  }

  @Override
  public UpdateResult executeUpdate() throws AdbcException {
    exportBind();
    long rowsAffected = JniLoader.INSTANCE.statementExecuteUpdate(handle);
    return new UpdateResult(rowsAffected);
  }

  @Override
  public void prepare() throws AdbcException {
    JniLoader.INSTANCE.statementPrepare(handle);
  }

  @Override
  public void close() {
    handle.close();
  }
}
