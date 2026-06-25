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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import org.apache.arrow.util.AutoCloseables;

/**
 * Track child resources for the ADBC FFI.
 *
 * <p>You are supposed to close statements before closing the connection (etc.). This class helps
 * track those references to prevent misuse at runtime.
 *
 * <p>This class is thread-safe.
 */
public final class ChildReferences implements AutoCloseable {
  private final Set<AutoCloseable> openReferences;

  public ChildReferences() {
    // TODO(lidavidm): we could use caffeine LoadingCache with weakKeys instead
    this.openReferences =
        Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));
  }

  public void close() throws Exception {
    // synchronizedSet requires explicit synchronization for iteration
    synchronized (openReferences) {
      try {
        var closeables = new ArrayList<>(openReferences);
        AutoCloseables.close(closeables);
      } finally {
        openReferences.clear();
      }
    }
  }

  public void addReference(AutoCloseable any) {
    openReferences.add(any);
  }

  public void releaseReference(AutoCloseable any) {
    openReferences.remove(any);
  }
}
