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
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;

namespace Apache.Arrow.Adbc.DriverManager
{
    /// <summary>
    /// The default filesystem-based <see cref="IConnectionProfileProvider"/>.
    /// Mirrors <c>AdbcProfileProviderFilesystem</c> in <c>adbc_driver_manager.h</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Profiles are loaded from TOML files using the following format:
    /// </para>
    /// <code>
    /// profile_version = 1
    /// driver = "driver_name"
    ///
    /// [Options]
    /// option1 = "value1"
    /// option2 = 42
    /// option3 = 3.14
    /// </code>
    /// <para>
    /// For backward compatibility, the legacy field names <c>version</c> and the
    /// <c>[options]</c> section are also accepted.
    /// </para>
    /// <para>
    /// Profiles are searched in the following order:
    /// </para>
    /// <list type="number">
    ///   <item>
    ///     <description>
    ///       If <c>profileName</c> is an absolute path, it is used directly
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

        // New spec names
        private const string OptionsSectionNew = "Options";
        // Legacy names (for backward compatibility)
        private const string OptionsSectionLegacy = "options";

        /// <summary>
        /// Initializes a new instance of <see cref="FilesystemProfileProvider"/>.
        /// </summary>
        public FilesystemProfileProvider() { }

        /// <summary>
        /// Parses a TOML connection profile from the given content string.
        /// </summary>
        /// <param name="tomlContent">The raw TOML text to parse.</param>
        /// <returns>The loaded <see cref="ConnectionProfile"/>.</returns>
        /// <exception cref="ArgumentNullException">If <paramref name="tomlContent"/> is null.</exception>
        /// <exception cref="AdbcException">
        /// If the TOML content is missing the required <c>profile_version</c> field or
        /// has an unsupported version number.
        /// </exception>
        public static ConnectionProfile LoadFromContent(string tomlContent)
        {
            if (tomlContent == null)
            {
                throw new ArgumentNullException(nameof(tomlContent));
            }

            Dictionary<string, Dictionary<string, object>> sections = TomlParser.Parse(tomlContent);

            Dictionary<string, object> root = sections.TryGetValue("", out Dictionary<string, object>? r) ? r : new Dictionary<string, object>();

            ValidateVersion(root);

            string? driverName = null;
            if (root.TryGetValue("driver", out object? driverObj) && driverObj is string driverStr)
            {
                driverName = driverStr;
            }

            string? driverTypeName = null;
            if (root.TryGetValue("driver_type", out object? driverTypeObj) && driverTypeObj is string driverTypeStr)
            {
                driverTypeName = driverTypeStr;
            }

            Dictionary<string, string> stringOpts = new Dictionary<string, string>(StringComparer.Ordinal);
            Dictionary<string, long> intOpts = new Dictionary<string, long>(StringComparer.Ordinal);
            Dictionary<string, double> doubleOpts = new Dictionary<string, double>(StringComparer.Ordinal);

            // Try new section name first, fall back to legacy
            Dictionary<string, object>? optSection = null;
            if (!sections.TryGetValue(OptionsSectionNew, out optSection))
            {
                sections.TryGetValue(OptionsSectionLegacy, out optSection);
            }

            if (optSection != null)
            {
                foreach (KeyValuePair<string, object> kv in optSection)
                {
                    string key = kv.Key;
                    object val = kv.Value;

                    switch (val)
                    {
                        case long lv:
                            intOpts[key] = lv;
                            break;
                        case double dv:
                            doubleOpts[key] = dv;
                            break;
                        case bool bv:
                            stringOpts[key] = bv ? "true" : "false";
                            break;
                        default:
                            stringOpts[key] = Convert.ToString(val, CultureInfo.InvariantCulture) ?? string.Empty;
                            break;
                    }
                }
            }

            return new ConnectionProfile(driverName, driverTypeName, stringOpts, intOpts, doubleOpts);
        }

        /// <summary>
        /// Parses a TOML connection profile from the given file path.
        /// </summary>
        /// <param name="filePath">Absolute or relative path to the <c>.toml</c> file.</param>
        /// <returns>The loaded <see cref="ConnectionProfile"/>.</returns>
        public static ConnectionProfile LoadFromFile(string filePath)
        {
            if (filePath == null)
            {
                throw new ArgumentNullException(nameof(filePath));
            }
            string content = File.ReadAllText(filePath, System.Text.Encoding.UTF8);
            return LoadFromContent(content);
        }

        /// <inheritdoc/>
        public ConnectionProfile? GetProfile(string profileName, string? additionalSearchPathList = null)
        {
            if (profileName == null)
            {
                throw new ArgumentNullException(nameof(profileName));
            }

            // If already an absolute path, load directly.
            if (IsAbsolutePath(profileName))
            {
                string candidate = EnsureTomlExtension(profileName);
                if (File.Exists(candidate))
                {
                    return LoadFromFile(candidate);
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
                    return LoadFromFile(candidate);
                }
            }

            return null;
        }

        private static void ValidateVersion(Dictionary<string, object> root)
        {
            // Try new field name first, fall back to legacy
            object? versionObj = null;
            if (!root.TryGetValue("profile_version", out versionObj))
            {
                root.TryGetValue("version", out versionObj);
            }

            if (versionObj == null)
            {
                throw new AdbcException(
                    "TOML profile is missing the required 'profile_version' field.",
                    AdbcStatusCode.InvalidArgument);
            }

            long version;
            if (versionObj is long lv)
            {
                version = lv;
            }
            else
            {
                try
                {
                    version = Convert.ToInt64(versionObj, CultureInfo.InvariantCulture);
                }
                catch (Exception ex) when (ex is FormatException || ex is InvalidCastException || ex is OverflowException)
                {
                    throw new AdbcException(
                        $"The 'profile_version' field has an invalid value '{versionObj}'. It must be an integer.",
                        AdbcStatusCode.InvalidArgument);
                }
            }

            if (version != 1)
            {
                throw new AdbcException(
                    $"Unsupported profile version '{version}'. Only version 1 is supported.",
                    AdbcStatusCode.NotImplemented);
            }
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

        /// <summary>
        /// Returns <c>true</c> when <paramref name="path"/> is fully qualified.
        /// On modern .NET this uses the stricter <c>Path.IsPathFullyQualified</c>;
        /// on .NET Framework / .NET Standard 2.0 it falls back to
        /// <see cref="Path.IsPathRooted(string)"/>.
        /// </summary>
        private static bool IsAbsolutePath(string path)
        {
#if NET6_0_OR_GREATER
            return Path.IsPathFullyQualified(path);
#else
            return Path.IsPathRooted(path);
#endif
        }

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
