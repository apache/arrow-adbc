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
  /**
   * The driver to load.
   *
   * <p>Can be a path to a driver, a name (which will be found via dlopen or equivalent), or a
   * manifest name (which will be loaded from platform-specific paths).
   */
  public static final TypedKey<String> PARAM_DRIVER = new TypedKey<>("jni.driver", String.class);

  /** The profile to load. */
  public static final TypedKey<String> PARAM_PROFILE = new TypedKey<>("jni.profile", String.class);

  /** Additional paths to search for driver manifests. */
  public static final TypedKey<String> PARAM_MANIFEST_SEARCH_PATH =
      new TypedKey<>("jni.additional_manifest_search_path_list", String.class);

  /** Additional paths to search for connection profiles. */
  public static final TypedKey<String> PARAM_PROFILE_SEARCH_PATH =
      new TypedKey<>("jni.additional_profile_search_path_list", String.class);

  static final TypedKey<Boolean> AUTOCOMMIT =
      new TypedKey<>("adbc.connection.autocommit", Boolean.class);
  static final TypedKey<String> ISOLATION_LEVEL =
      new TypedKey<>("adbc.connection.transaction.isolation_level", String.class);
  static final TypedKey<Boolean> READONLY =
      new TypedKey<>("adbc.connection.readonly", Boolean.class);
  static final String ISOLATION_LEVEL_READ_UNCOMMITTED =
      "adbc.connection.transaction.isolation.read_uncommitted";
  static final String ISOLATION_LEVEL_READ_COMMITTED =
      "adbc.connection.transaction.isolation.read_committed";
  static final String ISOLATION_LEVEL_REPEATABLE_READ =
      "adbc.connection.transaction.isolation.repeatable_read";
  static final String ISOLATION_LEVEL_SNAPSHOT = "adbc.connection.transaction.isolation.snapshot";
  static final String ISOLATION_LEVEL_SERIALIZABLE =
      "adbc.connection.transaction.isolation.serializable";

  static final TypedKey<String> CURRENT_CATALOG =
      new TypedKey<>("adbc.connection.catalog", String.class);
  static final TypedKey<String> CURRENT_DB_SCHEMA =
      new TypedKey<>("adbc.connection.db_schema", String.class);

  private final BufferAllocator allocator;

  public JniDriver(BufferAllocator allocator) {
    this.allocator = Objects.requireNonNull(allocator);
  }

  @Override
  public AdbcDatabase open(Map<String, Object> parameters) throws AdbcException {
    Map<String, String> nativeParameters = new HashMap<>();
    for (Map.Entry<String, Object> param : parameters.entrySet()) {
      if (param.getValue() instanceof String) {
        String key = param.getKey();
        if (key.startsWith("jni.")) {
          key = key.substring(4);
        }
        nativeParameters.put(key, (String) param.getValue());
      } else {
        throw AdbcException.invalidArgument("[jni] only String parameters are supported");
      }
    }

    NativeDatabaseHandle handle = JniLoader.INSTANCE.openDatabase(nativeParameters);
    return new JniDatabase(allocator, handle);
  }
}
