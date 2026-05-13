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
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.PartitionDescriptor;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

public class NativePartitionResult {
  private final List<PartitionDescriptor> partitions;
  private final long rowsAffected;
  private final ArrowSchema.Snapshot schemaSnapshot;

  public NativePartitionResult(long rowsAffected, long cDataSchema) {
    this.partitions = new ArrayList<>();
    this.rowsAffected = rowsAffected;
    // Immediately snapshot the stream to take ownership of the contents.
    // The address may point to a stack-allocated struct that becomes invalid
    // after the JNI call returns.
    this.schemaSnapshot = ArrowSchema.wrap(cDataSchema).snapshot();
  }

  /** For use by JNI code only. */
  public void addPartition(byte[] partition) {
    this.partitions.add(new PartitionDescriptor(ByteBuffer.wrap(partition)));
  }

  public AdbcStatement.PartitionResult importResult(BufferAllocator allocator) {
    try (final ArrowSchema schemaHandle = ArrowSchema.allocateNew(allocator)) {
      // It's possible the driver doesn't give us a schema.
      schemaHandle.save(schemaSnapshot);
      // TODO(lidavidm): work out dictionaries
      Schema schema = null;
      if (schemaSnapshot.release != 0) {
        schema = Data.importSchema(allocator, schemaHandle, null);
      }
      return new AdbcStatement.PartitionResult(schema, rowsAffected, partitions);
    }
  }
}
