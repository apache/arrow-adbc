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

/** A handle to an ADBC database driver. */
public interface AdbcDriver {
  /**
   * The standard parameter name for a password (type String).
   *
   * @since ADBC API revision 1.1.0
   */
  AdbcOptionKey<String> PARAM_PASSWORD = new AdbcOptionKey<>("password", String.class);

  /**
   * The standard parameter name for a connection URI (type String).
   *
   * @since ADBC API revision 1.1.0
   */
  AdbcOptionKey<String> PARAM_URI = new AdbcOptionKey<>("uri", String.class);

  /**
   * The standard parameter name for a connection URL (type String).
   *
   * @deprecated Prefer {@link #PARAM_URI} instead.
   */
  @Deprecated String PARAM_URL = "adbc.url";

  /**
   * The standard parameter name for a username (type String).
   *
   * @since ADBC API revision 1.1.0
   */
  AdbcOptionKey<String> PARAM_USERNAME = new AdbcOptionKey<>("username", String.class);

  /** The standard parameter name for SQL quirks configuration (type SqlQuirks). */
  String PARAM_SQL_QUIRKS = "adbc.sql.quirks";

  /** ADBC API revision 1.0.0. */
  long ADBC_VERSION_1_0_0 = 1_000_000;
  /** ADBC API revision 1.1.0. */
  long ADBC_VERSION_1_1_0 = 1_001_000;

  /**
   * Open a database via this driver.
   *
   * @param parameters Driver-specific parameters.
   */
  AdbcDatabase open(Map<String, Object> parameters) throws AdbcException;
}
