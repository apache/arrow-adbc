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

package org.apache.arrow.adbc.driver.flightsql;

import java.util.Collections;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/** An AdbcStatement implementation that returns fixed data from a root. */
class FixedRootStatement implements AdbcStatement {
  private final BufferAllocator allocator;
  private final Schema overrideSchema;
  private ArrowRecordBatch recordBatch;

  public FixedRootStatement(BufferAllocator allocator, VectorSchemaRoot root) {
    this.allocator = allocator;
    this.overrideSchema = root.getSchema();
    // Unload the root to preserve the data
    recordBatch = new VectorUnloader(root).getRecordBatch();
  }

  @Override
  public void execute() throws AdbcException {
    throw AdbcException.invalidState("[Flight SQL] Cannot execute() this statement");
  }

  @Override
  public ArrowReader getArrowReader() throws AdbcException {
    final ArrowReader reader =
        new RootArrowReader(allocator, overrideSchema, Collections.singletonList(recordBatch));
    recordBatch = null;
    return reader;
  }

  @Override
  public void prepare() throws AdbcException {
    throw AdbcException.invalidState("[Flight SQL] Cannot execute() this statement");
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(recordBatch);
  }
}
