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
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.adbc.driver.jni.impl.JniLoader;
import org.apache.arrow.adbc.driver.jni.impl.NativeDatabaseHandle;
import org.apache.arrow.memory.BufferAllocator;

public class JniDatabase implements AdbcDatabase {
  private final BufferAllocator allocator;
  private final NativeDatabaseHandle handle;

  public JniDatabase(BufferAllocator allocator, NativeDatabaseHandle handle) {
    this.allocator = allocator;
    this.handle = handle;
  }

  @Override
  public AdbcConnection connect() throws AdbcException {
    return new JniConnection(allocator, JniLoader.INSTANCE.openConnection(handle));
  }

  @Override
  public void close() {
    handle.close();
  }

  @Override
  public <T> T getOption(TypedKey<T> key) throws AdbcException {
    if (key.getType() == String.class) {
      return key.cast(JniLoader.INSTANCE.databaseGetOptionString(handle, key.getKey()));
    } else if (key.getType() == Integer.class) {
      return key.cast((int) JniLoader.INSTANCE.databaseGetOptionLong(handle, key.getKey()));
    } else if (key.getType() == Long.class) {
      return key.cast(JniLoader.INSTANCE.databaseGetOptionLong(handle, key.getKey()));
    } else if (key.getType() == Float.class) {
      return key.cast((float) JniLoader.INSTANCE.databaseGetOptionDouble(handle, key.getKey()));
    } else if (key.getType() == Double.class) {
      return key.cast(JniLoader.INSTANCE.databaseGetOptionDouble(handle, key.getKey()));
    } else if (key.getType() == Boolean.class) {
      String value = JniLoader.INSTANCE.databaseGetOptionString(handle, key.getKey());
      if (value == null) {
        return null;
      } else if ("true".equalsIgnoreCase(value)) {
        return key.cast(Boolean.TRUE);
      } else if ("false".equalsIgnoreCase(value)) {
        return key.cast(Boolean.FALSE);
      } else {
        throw AdbcException.invalidArgument(
            "[jni] invalid boolean value for option " + key.getKey() + ": " + value);
      }
    } else if (key.getType() == byte[].class) {
      return key.cast(JniLoader.INSTANCE.databaseGetOptionBytes(handle, key.getKey()));
    }
    return AdbcDatabase.super.getOption(key);
  }

  @Override
  public <T> void setOption(TypedKey<T> key, T value) throws AdbcException {
    if (value instanceof String) {
      JniLoader.INSTANCE.databaseSetOptionString(handle, key.getKey(), (String) value);
    } else if (value == null) {
      JniLoader.INSTANCE.databaseSetOptionString(handle, key.getKey(), null);
    } else if (value instanceof Integer) {
      JniLoader.INSTANCE.databaseSetOptionLong(handle, key.getKey(), (Integer) value);
    } else if (value instanceof Long) {
      JniLoader.INSTANCE.databaseSetOptionLong(handle, key.getKey(), (Long) value);
    } else if (value instanceof Float) {
      JniLoader.INSTANCE.databaseSetOptionDouble(handle, key.getKey(), (Float) value);
    } else if (value instanceof Double) {
      JniLoader.INSTANCE.databaseSetOptionDouble(handle, key.getKey(), (Double) value);
    } else if (value instanceof Boolean) {
      JniLoader.INSTANCE.databaseSetOptionString(
          handle, key.getKey(), ((Boolean) value) ? "true" : "false");
    } else if (value instanceof byte[]) {
      JniLoader.INSTANCE.databaseSetOptionBytes(handle, key.getKey(), (byte[]) value);
    } else {
      throw AdbcException.invalidArgument(
          "[jni] unsupported database option type " + value.getClass());
    }
  }
}
