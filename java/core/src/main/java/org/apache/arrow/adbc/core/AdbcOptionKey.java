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

import java.util.Map;
import java.util.Objects;

/**
 * A typesafe option key.
 *
 * @since ADBC API revision 1.1.0
 * @param <T> The option value type.
 */
public final class AdbcOptionKey<T> {
  private final String key;
  private final Class<T> type;

  public AdbcOptionKey(String key, Class<T> type) {
    this.key = Objects.requireNonNull(key);
    this.type = Objects.requireNonNull(type);
  }

  /**
   * Set this option in an options map (like for {@link AdbcDriver#open(Map)}.
   *
   * @param options The options.
   * @param value The option value.
   */
  public void set(Map<String, Object> options, T value) {
    options.put(key, value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AdbcOptionKey<?> that = (AdbcOptionKey<?>) o;
    return Objects.equals(key, that.key) && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, type);
  }

  @Override
  public String toString() {
    return "AdbcOptionKey{" + key + ", " + type + '}';
  }
}
