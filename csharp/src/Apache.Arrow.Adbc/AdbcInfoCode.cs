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
        /// The database vendor/product name (e.g. the server name).
        /// </summary>
        VendorName = 0,

        /// <summary>
        /// The database vendor/product version
        /// </summary>
        VendorVersion = 1,

        /// <summary>
        /// The database vendor/product Arrow library version
        /// </summary>
        VendorArrowVersion = 2,

        /// <summary>
        /// The driver name
        /// </summary>
        DriverName = 100,

        /// <summary>
        /// The driver version
        /// </summary>
        DriverVersion = 101,

        /// <summary>
        /// The driver Arrow library version
        /// </summary>
        DriverArrowVersion = 102
    }
}
