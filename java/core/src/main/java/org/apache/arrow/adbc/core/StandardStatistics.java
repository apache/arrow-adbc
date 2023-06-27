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

/**
 * Definitions of standard statistic names/keys.
 *
 * <p>Statistic names are returned from {@link AdbcConnection#getStatistics(String, String, String,
 * boolean)} in a dictionary-encoded form. This class provides the names and dictionary-encoded form
 * of statistics defined by ADBC.
 */
public enum StandardStatistics {
  /**
   * The average byte width statistic. The average size in bytes of a row in the column. Value type
   * is float64.
   *
   * <p>For example, this is roughly the average length of a string for a string column.
   */
  AVERAGE_BYTE_WIDTH("adbc.statistic.byte_width", 0),
  /**
   * The distinct value count (NDV) statistic. The number of distinct values in the column. Value
   * type is int64 (when not approximate) or float64 (when approximate).
   */
  DISTINCT_COUNT("adbc.statistic.distinct_count", 1),
  /**
   * The max byte width statistic. The maximum size in bytes of a row in the column. Value type is
   * int64 (when not approximate) or float64 (when approximate).
   *
   * <p>For example, this is the maximum length of a string for a string column.
   */
  MAX_BYTE_WIDTH("adbc.statistic.byte_width", 2),
  /** The max value statistic. Value type is column-dependent. */
  MAX_VALUE_NAME("adbc.statistic.byte_width", 3),
  /** The min value statistic. Value type is column-dependent. */
  MIN_VALUE_NAME("adbc.statistic.byte_width", 4),
  /**
   * The null count statistic. The number of values that are null in the column. Value type is int64
   * (when not approximate) or float64 (when approximate).
   */
  NULL_COUNT_NAME("adbc.statistic.null_count", 5),
  /**
   * The row count statistic. The number of rows in the column or table. Value type is int64 (when
   * not approximate) or float64 (when approximate).
   */
  ROW_COUNT_NAME("adbc.statistic.row_count", 6),
  ;

  private final String name;
  private final int key;

  StandardStatistics(String name, int key) {
    this.name = Objects.requireNonNull(name);
    this.key = key;
  }

  /** Get the statistic name. */
  public String getName() {
    return name;
  }

  /** Get the dictionary-encoded name. */
  public int getKey() {
    return key;
  }
}
