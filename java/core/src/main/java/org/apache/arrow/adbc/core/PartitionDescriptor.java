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

import java.nio.ByteBuffer;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An opaque descriptor for a part of a potentially distributed or partitioned result set. */
public final class PartitionDescriptor {
  private final ByteBuffer descriptor;

  public PartitionDescriptor(final ByteBuffer descriptor) {
    this.descriptor = Objects.requireNonNull(descriptor);
  }

  public ByteBuffer getDescriptor() {
    return descriptor;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionDescriptor that = (PartitionDescriptor) o;
    return descriptor.equals(that.descriptor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(descriptor);
  }

  @Override
  public String toString() {
    return "PartitionDescriptor{" + "descriptor=" + descriptor + '}';
  }
}
