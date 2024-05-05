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
    /// <summary>
    /// Info codes used for GetInfo
    /// </summary>
    public enum AdbcInfoCode
    {
        /// <summary>
        /// The database vendor/product name (e.g. the server name). Type: string.
        /// </summary>
        VendorName = 0,

        /// <summary>
        /// The database vendor/product version. Type: string.
        /// </summary>
        VendorVersion = 1,

        /// <summary>
        /// The database vendor/product Arrow library version. Type: string.
        /// </summary>
        VendorArrowVersion = 2,

        /// <summary>
        /// Whether or not SQL queries are supported. Type: bool.
        /// </summary>
        VendorSql = 3,

        /// <summary>
        /// Whether or not Substrait queries are supported. Type: bool.
        /// </summary>
        VendorSubstrait = 4,

        /// <summary>
        /// The minimum supported Substrait version, or null if not supported. Type: string.
        /// </summary>
        VendorSubstraitMinVersion = 5,

        /// <summary>
        /// The maximum supported Substrait version, or null if not supported. Type: string.
        /// </summary>
        VendorSubstraitMaxVersion = 6,

        /// <summary>
        /// The driver name. Type: string.
        /// </summary>
        DriverName = 100,

        /// <summary>
        /// The driver version. Type: string.
        /// </summary>
        DriverVersion = 101,

        /// <summary>
        /// The driver Arrow library version. Type: string.
        /// </summary>
        DriverArrowVersion = 102,

        /// <summary>
        /// The driver ADBC API version. Type: int64.
        /// </summary>
        /// <remarks>
        /// The value should be one of the ADBC_VERSION...
        ///
        /// Added in ADBC 1.1.0.
        /// </remarks>
        DriverAdbcVersion = 103,
    }
}
