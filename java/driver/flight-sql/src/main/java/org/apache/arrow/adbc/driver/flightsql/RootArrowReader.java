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

import java.io.IOException;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/** An ArrowReader that wraps a list of ArrowRecordBatches. */
class RootArrowReader extends ArrowReader {
  private final Schema schema;
  private final List<ArrowRecordBatch> batches;
  int nextIndex;

  public RootArrowReader(BufferAllocator allocator, Schema schema, List<ArrowRecordBatch> batches) {
    super(allocator);
    this.schema = schema;
    this.batches = batches;
    this.nextIndex = 0;
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    if (nextIndex < batches.size()) {
      new VectorLoader(getVectorSchemaRoot()).load(batches.get(nextIndex++));
      return true;
    }
    return false;
  }

  @Override
  public long bytesRead() {
    return 0;
  }

  @Override
  protected void closeReadSource() throws IOException {
    try {
      AutoCloseables.close(batches);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected Schema readSchema() {
    return schema;
  }
}
