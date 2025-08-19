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

import java.lang.ref.Cleaner;
import org.apache.arrow.adbc.core.AdbcException;

/** A wrapper around a C-allocated ADBC resource. */
abstract class NativeHandle implements AutoCloseable {
  static final Cleaner cleaner = Cleaner.create();

  protected final State state;
  private final Cleaner.Cleanable cleanable;

  NativeHandle(long nativeHandle) {
    this.state = new State(nativeHandle, getCloseFunction());
    this.cleanable = cleaner.register(this, state);
  }

  /** Get the native function used to free the resource. */
  abstract Closer getCloseFunction();

  @Override
  public void close() {
    cleanable.clean();
  }

  protected static class State implements Runnable {
    long nativeHandle;
    private final Closer closer;

    State(long nativeHandle, Closer closer) {
      this.nativeHandle = nativeHandle;
      this.closer = closer;
    }

    @Override
    public void run() {
      if (nativeHandle == 0) return;
      final long handle = nativeHandle;
      nativeHandle = 0;
      try {
        closer.close(handle);
      } catch (AdbcException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @FunctionalInterface
  interface Closer {
    void close(long handle) throws AdbcException;
  }
}
