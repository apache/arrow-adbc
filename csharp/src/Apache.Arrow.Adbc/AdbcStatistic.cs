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

namespace Apache.Arrow.Adbc
{
    public static class AdbcStatistic
    {
        // TODO: Rethink how this is stored

        /// <summary>
        /// The average byte width statistic.  The average size in bytes of a
        /// row in the column.  Value type is float64.
        ///
        /// For example, this is roughly the average length of a string for a string
        /// column.
        /// </summary>
        public const string AverageByteWidthName = "adbc.statistic.byte_width";
        public const int AverageByteWidthKey = 0;

        /// <summary>
        /// The distinct value count (NDV) statistic.  The number of distinct
        /// values in the column.  Value type is int64 (when not approximate) or
        /// float64 (when approximate).
        /// </summary>
        public const string DistinctCountName = "adbc.statistic.distinct_count";
        public const int DistinctCountKey = 1;

        /// <summary>
        /// The max byte width statistic.  The maximum size in bytes of a row
        /// in the column.  Value type is int64 (when not approximate) or float64
        /// (when approximate).
        ///
        /// For example, this is the maximum length of a string for a string column.
        /// </summary>
        public const string MaxByteWidthName = "adbc.statistic.max_byte_width";
        public const int MaxByteWidthKey = 2;

        /// <summary>
        /// The max value statistic.  Value type is column-dependent.
        /// </summary>
        public const string MaxValueName = "adbc.statistic.max_value";
        public const int MaxValueKey = 3;

        /// <summary>
        /// The min value statistic.  Value type is column-dependent.
        /// </summary>
        public const string MinValueName = "adbc.statistic.min_value";
        public const int MinValueKey = 4;

        /// <summary>
        /// The null count statistic.  The number of values that are null in
        /// the column.  Value type is int64 (when not approximate) or float64
        /// (when approximate).
        /// </summary>
        public const string NullCountName = "adbc.statistic.null_count";
        public const int NullCountKey = 5;

        /// <summary>
        /// The row count statistic.  The number of rows in the column or
        /// table.  Value type is int64 (when not approximate) or float64 (when
        /// approximate).
        /// </summary>
        public const string RowCountName = "adbc.statistic.row_count";
        public const int RowCountKey = 6;
    }
}
