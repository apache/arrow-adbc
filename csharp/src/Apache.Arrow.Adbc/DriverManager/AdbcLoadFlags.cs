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

using System;

namespace Apache.Arrow.Adbc.DriverManager
{
    /// <summary>
    /// Flags to control how the ADBC driver manager searches for and loads drivers.
    /// Mirrors the <c>AdbcLoadFlags</c> type and <c>ADBC_LOAD_FLAG_*</c> constants
    /// defined in <c>adbc_driver_manager.h</c>.
    /// </summary>
    [Flags]
    public enum AdbcLoadFlags : uint
    {
        /// <summary>No search flags set.</summary>
        None = 0,

        /// <summary>
        /// Search the <c>ADBC_DRIVER_PATH</c> environment variable for drivers.
        /// Corresponds to <c>ADBC_LOAD_FLAG_SEARCH_ENV</c>.
        /// </summary>
        SearchEnv = 1,

        /// <summary>
        /// Search the user-level configuration directory for drivers.
        /// Corresponds to <c>ADBC_LOAD_FLAG_SEARCH_USER</c>.
        /// </summary>
        SearchUser = 2,

        /// <summary>
        /// Search the system-level configuration directory for drivers.
        /// Corresponds to <c>ADBC_LOAD_FLAG_SEARCH_SYSTEM</c>.
        /// </summary>
        SearchSystem = 4,

        /// <summary>
        /// Allow loading drivers from relative paths.
        /// Corresponds to <c>ADBC_LOAD_FLAG_ALLOW_RELATIVE_PATHS</c>.
        /// </summary>
        AllowRelativePaths = 8,

        /// <summary>
        /// Default flags: search env, user, system, and allow relative paths.
        /// Corresponds to <c>ADBC_LOAD_FLAG_DEFAULT</c>.
        /// </summary>
        Default = SearchEnv | SearchUser | SearchSystem | AllowRelativePaths,
    }
}
