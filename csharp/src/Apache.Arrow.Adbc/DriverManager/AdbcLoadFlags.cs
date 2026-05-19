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
        /// <remarks>
        /// Even when set, relative paths are resolved against
        /// <see cref="AppContext.BaseDirectory"/> rather than the process current
        /// working directory, so this flag does not enable CWD-based loading.
        /// Disable this flag to require callers to supply fully-qualified paths or
        /// bare driver names that are resolved via the configured search
        /// directories.
        /// </remarks>
        AllowRelativePaths = 8,

        /// <summary>
        /// Default flags for this .NET driver manager: search env, user, and system
        /// directories. Relative-path loading is <b>not</b> enabled by default; callers
        /// that need it must opt in by combining <see cref="AllowRelativePaths"/> with
        /// the other flags (or use <see cref="Compatible"/>).
        /// </summary>
        /// <remarks>
        /// This intentionally diverges from <c>ADBC_LOAD_FLAG_DEFAULT</c> in
        /// <c>adbc_driver_manager.h</c>, which includes
        /// <c>ADBC_LOAD_FLAG_ALLOW_RELATIVE_PATHS</c>. The C# default is hardened to
        /// reduce attack surface from accidentally loading a driver out of an
        /// attacker-influenced directory. Use <see cref="Compatible"/> to opt in to
        /// the C header's exact default flag set.
        /// </remarks>
        Default = SearchEnv | SearchUser | SearchSystem,

        /// <summary>
        /// Flag set that mirrors <c>ADBC_LOAD_FLAG_DEFAULT</c> from
        /// <c>adbc_driver_manager.h</c> exactly, including
        /// <see cref="AllowRelativePaths"/>. Prefer <see cref="Default"/> unless
        /// strict parity with the C header is required.
        /// </summary>
        Compatible = SearchEnv | SearchUser | SearchSystem | AllowRelativePaths,
    }
}
