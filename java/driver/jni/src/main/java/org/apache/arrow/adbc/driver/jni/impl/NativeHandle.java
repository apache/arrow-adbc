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

public class NativeHandle implements AutoCloseable {
  static final Cleaner cleaner = Cleaner.create();

  private final State state;
  private final Cleaner.Cleanable cleanable;

  NativeHandle(NativeHandleType type, long nativeHandle) {
    this.state = new State(type, nativeHandle);
    this.cleanable = cleaner.register(this, state);
  }

  @Override
  public void close() {
    cleanable.clean();
  }

  public NativeHandleType getHandleType() {
    return state.type;
  }

  long getHandle() {
    return state.nativeHandle;
  }

  private static class State implements Runnable {
    private final NativeHandleType type;
    long nativeHandle;

    State(NativeHandleType type, long nativeHandle) {
      this.type = type;
      this.nativeHandle = nativeHandle;
    }

    @Override
    public void run() {
      if (nativeHandle == 0) return;
      final long handle = nativeHandle;
      nativeHandle = 0;
      try {
        switch (type) {
          case DATABASE:
            NativeAdbc.closeDatabase(handle);
            break;
          case CONNECTION:
            NativeAdbc.closeConnection(handle);
            break;
          case STATEMENT:
            NativeAdbc.closeStatement(handle);
            break;
        }
      } catch (NativeAdbcException e) {
        // TODO: convert to ADBC exception first
        throw new RuntimeException(e);
      }
    }
  }
}
