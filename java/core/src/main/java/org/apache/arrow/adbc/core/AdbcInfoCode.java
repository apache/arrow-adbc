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

/** Integer IDs used for requesting information about the database/driver. */
public enum AdbcInfoCode {
  /** The database vendor/product name (e.g. the server name) (type: utf8). */
  VENDOR_NAME(0),
  /** The database vendor/product version (type: utf8). */
  VENDOR_VERSION(1),
  /** The database vendor/product Arrow library version (type: utf8). */
  VENDOR_ARROW_VERSION(2),

  /** The driver name (type: utf8). */
  DRIVER_NAME(100),
  /** The driver version (type: utf8). */
  DRIVER_VERSION(101),
  /** The driver Arrow library version (type: utf8). */
  DRIVER_ARROW_VERSION(102),
  ;

  private final int value;

  AdbcInfoCode(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
