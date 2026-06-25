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

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A proxy {@link ArrowReader} that keeps an associated ADBC resource alive. */
class TiedArrowReader extends ArrowReader {
  private final ArrowReader delegate;
  private @Nullable HasChildReferences parent;

  TiedArrowReader(
      BufferAllocator allocator, ArrowReader delegate, @Nullable HasChildReferences parent) {
    // XXX: ArrowReader being an abstract class and not an interface is a massive wart in arrow-java
    // design
    super(allocator);
    this.delegate = delegate;
    this.parent = parent;
    if (parent != null) {
      parent.getChildReferences().addReference(this);
    }
  }

  @Override
  public void close(boolean closeReadSource) throws IOException {
    try {
      delegate.close(closeReadSource);
    } finally {
      // release even if we couldn't close properly
      if (parent != null) {
        parent.getChildReferences().releaseReference(this);
      }
      parent = null;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      delegate.close();
    } finally {
      if (parent != null) {
        parent.getChildReferences().releaseReference(this);
      }
      parent = null;
    }
  }

  @Override
  public long bytesRead() {
    return delegate.bytesRead();
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    return delegate.loadNextBatch();
  }

  @Override
  public Set<Long> getDictionaryIds() {
    return delegate.getDictionaryIds();
  }

  @Override
  public Dictionary lookup(long id) {
    return delegate.lookup(id);
  }

  @Override
  public Map<Long, Dictionary> getDictionaryVectors() throws IOException {
    return delegate.getDictionaryVectors();
  }

  @Override
  public VectorSchemaRoot getVectorSchemaRoot() throws IOException {
    return delegate.getVectorSchemaRoot();
  }

  @Override
  protected void closeReadSource() {
    // Not actually called because we delegate all public methods
    throw new AssertionError();
  }

  @Override
  protected Schema readSchema() throws IOException {
    // Not actually called because we delegate all public methods
    throw new AssertionError();
  }
}
