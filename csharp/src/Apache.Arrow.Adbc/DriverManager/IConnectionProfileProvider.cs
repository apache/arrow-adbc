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

namespace Apache.Arrow.Adbc.DriverManager
{
    /// <summary>
    /// Provides <see cref="IConnectionProfile"/> instances by name.
    /// Mirrors the <c>AdbcConnectionProfileProvider</c> function type defined in
    /// <c>adbc_driver_manager.h</c>.
    /// </summary>
    public interface IConnectionProfileProvider
    {
        /// <summary>
        /// Loads a connection profile by name.
        /// </summary>
        /// <param name="profileName">
        /// The name of the profile to load. May be an absolute file path or a bare name
        /// to be resolved by searching the configured directories.
        /// </param>
        /// <param name="additionalSearchPathList">
        /// An optional OS path-list-separator-delimited list of additional directories to
        /// search for profiles, or <c>null</c> to use only the default locations.
        /// </param>
        /// <returns>
        /// The loaded <see cref="IConnectionProfile"/>, or <c>null</c> if no profile with
        /// <paramref name="profileName"/> was found.
        /// </returns>
        IConnectionProfile? GetProfile(string profileName, string? additionalSearchPathList = null);
    }
}
