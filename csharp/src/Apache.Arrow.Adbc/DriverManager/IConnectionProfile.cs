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
using System.Collections.Generic;

namespace Apache.Arrow.Adbc.DriverManager
{
    /// <summary>
    /// Represents an ADBC connection profile, which specifies a driver and a set of
    /// options to apply when initializing a database connection.
    /// Mirrors the <c>AdbcConnectionProfile</c> struct defined in <c>adbc_driver_manager.h</c>.
    /// </summary>
    /// <remarks>
    /// The profile specifies a driver (optionally) and key/value options of three types:
    /// string, 64-bit integer, and double. String option values of the form
    /// <c>env_var(ENV_VAR_NAME)</c> are expanded from the named environment variable
    /// before being applied to the database.
    /// </remarks>
    public interface IConnectionProfile : IDisposable
    {
        /// <summary>
        /// Gets the name of the driver specified by this profile, or <c>null</c> if the
        /// profile does not specify a driver.
        /// For native drivers this is the path to a shared library or a bare driver name.
        /// For managed drivers this is the path to the .NET assembly.
        /// </summary>
        string? DriverName { get; }

        /// <summary>
        /// Gets the fully-qualified .NET type name of the <see cref="AdbcDriver"/> subclass
        /// to instantiate for managed (pure .NET) drivers, or <c>null</c> for native drivers.
        /// When set, <see cref="AdbcDriverManager.LoadManagedDriver"/> is used instead of
        /// the native <c>NativeLibrary</c> path.
        /// </summary>
        /// <example>
        /// <c>Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver</c>
        /// </example>
        string? DriverTypeName { get; }

        /// <summary>
        /// Gets the string options specified by this profile. Values of the form
        /// <c>env_var(ENV_VAR_NAME)</c> will be expanded from the named environment
        /// variable when applied.
        /// </summary>
        IReadOnlyDictionary<string, string> StringOptions { get; }

        /// <summary>
        /// Gets the 64-bit integer options specified by this profile.
        /// </summary>
        IReadOnlyDictionary<string, long> IntOptions { get; }

        /// <summary>
        /// Gets the double-precision floating-point options specified by this profile.
        /// </summary>
        IReadOnlyDictionary<string, double> DoubleOptions { get; }
    }
}
