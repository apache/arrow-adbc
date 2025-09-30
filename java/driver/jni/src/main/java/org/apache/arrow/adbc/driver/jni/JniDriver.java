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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.adbc.driver.jni.impl.JniLoader;
import org.apache.arrow.adbc.driver.jni.impl.NativeDatabaseHandle;
import org.apache.arrow.memory.BufferAllocator;

/** An ADBC driver wrapping Arrow Flight SQL. */
public class JniDriver implements AdbcDriver {
  public static final TypedKey<String> PARAM_DRIVER = new TypedKey<>("jni.driver", String.class);

  private final BufferAllocator allocator;

  public JniDriver(BufferAllocator allocator) {
    this.allocator = Objects.requireNonNull(allocator);
  }

  @Override
  public AdbcDatabase open(Map<String, Object> parameters) throws AdbcException {
    String driverName = PARAM_DRIVER.get(parameters);
    if (driverName == null) {
      throw AdbcException.invalidArgument(
          "[JNI] Must provide String " + PARAM_DRIVER + " parameter");
    }

    Map<String, String> nativeParameters = new HashMap<>();
    nativeParameters.put("driver", driverName);

    for (Map.Entry<String, Object> param : parameters.entrySet()) {
      if (param.getKey().equals(PARAM_DRIVER.getKey())) continue;

      if (param.getValue() instanceof String) {
        nativeParameters.put(param.getKey(), (String) param.getValue());
      } else {
        throw AdbcException.invalidArgument("[jni] only String parameters are supported");
      }
    }

    NativeDatabaseHandle handle = JniLoader.INSTANCE.openDatabase(nativeParameters);
    return new JniDatabase(allocator, handle);
  }
}
