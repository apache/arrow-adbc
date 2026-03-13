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
using System.IO;
using System.Runtime.InteropServices;

namespace Apache.Arrow.Adbc.DriverManager
{
    /// <summary>
    /// The default filesystem-based <see cref="IConnectionProfileProvider"/>.
    /// Mirrors <c>AdbcProfileProviderFilesystem</c> in <c>adbc_driver_manager.h</c>.
    /// </summary>
    /// <remarks>
    /// Profiles are searched in the following order:
    /// <list type="number">
    ///   <item>
    ///     <description>
    ///       If <paramref name="profileName"/> is an absolute path, it is used directly
    ///       (with a <c>.toml</c> extension appended if none is present).
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       The directories supplied in <c>additionalSearchPathList</c> (delimited by the
    ///       OS path-list separator, e.g. <c>;</c> on Windows, <c>:</c> on POSIX).
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       The directories listed in the <c>ADBC_PROFILE_PATH</c> environment variable.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>The user-level configuration directory (e.g. <c>~/.config/adbc/profiles</c>).</description>
    ///   </item>
    /// </list>
    /// </remarks>
    public sealed class FilesystemProfileProvider : IConnectionProfileProvider
    {
        /// <summary>
        /// The environment variable that specifies additional profile search paths.
        /// </summary>
        public const string ProfilePathEnvVar = "ADBC_PROFILE_PATH";

        /// <summary>
        /// Initializes a new instance of <see cref="FilesystemProfileProvider"/>.
        /// </summary>
        public FilesystemProfileProvider() { }

        /// <inheritdoc/>
        public IConnectionProfile? GetProfile(string profileName, string? additionalSearchPathList = null)
        {
            if (profileName == null)
            {
                throw new ArgumentNullException(nameof(profileName));
            }

            // If already an absolute path, load directly.
            if (Path.IsPathRooted(profileName))
            {
                string candidate = EnsureTomlExtension(profileName);
                if (File.Exists(candidate))
                {
                    return TomlConnectionProfile.FromFile(candidate);
                }
                return null;
            }

            // Build the ordered search directories.
            List<string> dirs = new List<string>();

            if (!string.IsNullOrEmpty(additionalSearchPathList))
            {
                dirs.AddRange(SplitPathList(additionalSearchPathList!));
            }

            string? profilePathEnv = Environment.GetEnvironmentVariable(ProfilePathEnvVar);
            if (!string.IsNullOrEmpty(profilePathEnv))
            {
                dirs.AddRange(SplitPathList(profilePathEnv!));
            }

            string userDir = GetUserProfileDirectory();
            if (!string.IsNullOrEmpty(userDir))
            {
                dirs.Add(userDir);
            }

            string fileName = EnsureTomlExtension(profileName);

            foreach (string dir in dirs)
            {
                string candidate = Path.Combine(dir, fileName);
                if (File.Exists(candidate))
                {
                    return TomlConnectionProfile.FromFile(candidate);
                }
            }

            return null;
        }

        private static string EnsureTomlExtension(string name)
        {
            string ext = Path.GetExtension(name);
            return string.IsNullOrEmpty(ext)
                ? name + ".toml"
                : name;
        }

        private static string[] SplitPathList(string pathList) =>
            pathList.Split(new char[] { Path.PathSeparator }, StringSplitOptions.RemoveEmptyEntries);

        private static string GetUserProfileDirectory()
        {
            // ~/.config/adbc/profiles  (Linux / macOS)
            // %APPDATA%\adbc\profiles  (Windows)
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                string? appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
                return string.IsNullOrEmpty(appData)
                    ? string.Empty
                    : Path.Combine(appData, "adbc", "profiles");
            }
            else
            {
                string home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
                return string.IsNullOrEmpty(home)
                    ? string.Empty
                    : Path.Combine(home, ".config", "adbc", "profiles");
            }
        }
    }
}
