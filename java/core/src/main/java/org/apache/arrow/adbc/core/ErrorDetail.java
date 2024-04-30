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

import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Additional details (not necessarily human-readable) contained in an {@link AdbcException}. */
public class ErrorDetail {
  private final String key;
  private final Object value;

  public ErrorDetail(String key, Object value) {
    this.key = Objects.requireNonNull(key);
    this.value = Objects.requireNonNull(value);
  }

  public String getKey() {
    return key;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ErrorDetail that = (ErrorDetail) o;
    return Objects.equals(getKey(), that.getKey()) && Objects.equals(getValue(), that.getValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getKey(), getValue());
  }

  @Override
  public String toString() {
    return "ErrorDetail{" + "key='" + key + '\'' + ", value=" + value + '}';
  }
}
