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

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.jni.impl.JniLoader;
import org.apache.arrow.adbc.driver.jni.impl.NativeConnectionHandle;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.checkerframework.checker.nullness.qual.Nullable;

public class JniConnection implements AdbcConnection {
  private final BufferAllocator allocator;
  private final NativeConnectionHandle handle;

  public JniConnection(BufferAllocator allocator, NativeConnectionHandle handle) {
    this.allocator = allocator;
    this.handle = handle;
  }

  @Override
  public AdbcStatement createStatement() throws AdbcException {
    return new JniStatement(allocator, JniLoader.INSTANCE.openStatement(handle));
  }

  @Override
  public ArrowReader getInfo(int @Nullable [] infoCodes) throws AdbcException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    handle.close();
  }
}
