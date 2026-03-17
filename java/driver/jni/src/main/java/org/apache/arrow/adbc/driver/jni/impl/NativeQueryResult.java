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

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

public class NativeQueryResult {
  private final long rowsAffected;
  private final ArrowArrayStream.Snapshot streamSnapshot;

  public NativeQueryResult(long rowsAffected, long cDataStream) {
    this.rowsAffected = rowsAffected;
    // Immediately snapshot the stream to take ownership of the contents.
    // The address may point to a stack-allocated struct that becomes invalid
    // after the JNI call returns.
    this.streamSnapshot = ArrowArrayStream.wrap(cDataStream).snapshot();
  }

  public long rowsAffected() {
    return rowsAffected;
  }

  /** Import the C Data stream into a Java ArrowReader. */
  public ArrowReader importStream(BufferAllocator allocator) {
    try (final ArrowArrayStream cStream = ArrowArrayStream.allocateNew(allocator)) {
      cStream.save(streamSnapshot);
      return Data.importArrayStream(allocator, cStream);
    }
  }
}
