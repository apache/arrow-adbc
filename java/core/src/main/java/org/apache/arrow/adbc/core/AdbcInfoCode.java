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

/**
 * Integer IDs used for requesting information about the database/driver.
 *
 * <p>Since ADBC 1.1.0: the range [500, 1_000) is reserved for "XDBC" information, which is the same
 * metadata provided by the same info code range in the Arrow Flight SQL GetSqlInfo RPC.
 */
public enum AdbcInfoCode {
  /** The database vendor/product name (e.g. the server name) (type: utf8). */
  VENDOR_NAME(0),
  /** The database vendor/product version (type: utf8). */
  VENDOR_VERSION(1),
  /** The database vendor/product Arrow library version (type: utf8). */
  VENDOR_ARROW_VERSION(2),
  /** Indicates whether SQL queries are supported (type: bool). */
  VENDOR_SQL(3),
  /** Indicates whether Substrait queries are supported (type: bool). */
  VENDOR_SUBSTRAIT(4),
  /**
   * The minimum supported Substrait version, or null if Substrait is not supported (type: utf8).
   */
  VENDOR_SUBSTRAIT_MIN_VERSION(5),
  /**
   * The maximum supported Substrait version, or null if Substrait is not supported (type: utf8).
   */
  VENDOR_SUBSTRAIT_MAX_VERSION(6),

  /** The driver name (type: utf8). */
  DRIVER_NAME(100),
  /** The driver version (type: utf8). */
  DRIVER_VERSION(101),
  /** The driver Arrow library version (type: utf8). */
  DRIVER_ARROW_VERSION(102),
  /**
   * The ADBC API version (type: int64).
   *
   * <p>The value should be one of the ADBC_VERSION constants.
   *
   * @see AdbcDriver#ADBC_VERSION_1_0_0
   * @see AdbcDriver#ADBC_VERSION_1_1_0
   * @since ADBC API revision 1.1.0
   */
  DRIVER_ADBC_VERSION(103),
  ;

  private final int value;

  AdbcInfoCode(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
