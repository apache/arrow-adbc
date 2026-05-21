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
using System.Text;

namespace Apache.Arrow.Adbc.DriverManager
{
    /// <summary>
    /// A parsed ADBC driver manifest. A driver manifest is a TOML file that
    /// describes <i>where</i> a driver shared library lives and how to load it.
    /// It is distinct from a <see cref="ConnectionProfile"/>, which describes
    /// <i>how to open a database</i> with a driver and carries option key/value
    /// pairs.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The manifest format is defined in
    /// <c>docs/source/format/driver_manifests.rst</c>:
    /// </para>
    /// <code>
    /// manifest_version = 1
    ///
    /// name = "Driver Display Name"
    /// version = "1.2.3"            # the driver's own version, a string
    /// publisher = "..."
    /// license = "Apache-2.0"
    /// source = "..."
    ///
    /// [Driver]
    /// entrypoint = "AdbcDriverInit"   # optional; defaults are derived from the file name
    ///
    /// # Either a single path:
    /// [Driver]
    /// shared = "/path/to/libadbc_driver.so"
    ///
    /// # Or platform-tuple-keyed paths:
    /// [Driver.shared]
    /// linux_amd64   = "/path/to/libadbc_driver.so"
    /// macos_arm64   = "/path/to/libadbc_driver.dylib"
    /// windows_amd64 = "C:\\path\\to\\adbc_driver.dll"
    /// </code>
    /// <para>
    /// The <see cref="Entrypoint"/> value may be a plain native symbol name
    /// (e.g. <c>AdbcDriverDuckdbInit</c>) or a scheme-prefixed value such as
    /// <c>dotnet:Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver</c> /
    /// <c>netfx:My.Driver.Class</c>. The scheme tells the driver manager which
    /// managed-runtime host to start; values without a scheme are loaded as
    /// native C entrypoints.
    /// </para>
    /// </remarks>
    internal sealed class DriverManifest
    {
        private const string ManifestVersionField = "manifest_version";

        private DriverManifest(
            long manifestVersion,
            string? name,
            string? version,
            string? publisher,
            string? license,
            string? source,
            string? entrypoint,
            string libraryPath)
        {
            ManifestVersion = manifestVersion;
            Name = name;
            Version = version;
            Publisher = publisher;
            License = license;
            Source = source;
            Entrypoint = entrypoint;
            LibraryPath = libraryPath;
        }

        /// <summary>The manifest format version. Always 1 today.</summary>
        public long ManifestVersion { get; }

        /// <summary>Display name of the driver.</summary>
        public string? Name { get; }

        /// <summary>The driver's own version (a free-form string per the spec).</summary>
        public string? Version { get; }

        /// <summary>Publisher of the driver.</summary>
        public string? Publisher { get; }

        /// <summary>License identifier of the driver.</summary>
        public string? License { get; }

        /// <summary>Where this driver came from (e.g. a package name).</summary>
        public string? Source { get; }

        /// <summary>
        /// The entrypoint value from <c>[Driver].entrypoint</c>. May be a plain
        /// symbol name, a <c>dotnet:</c>-prefixed type name, or any other
        /// scheme-prefixed value. May be <c>null</c> if the manifest does not
        /// specify one (in which case callers derive a default).
        /// </summary>
        public string? Entrypoint { get; }

        /// <summary>
        /// The driver library path for the current platform, resolved from
        /// <c>[Driver.shared]</c>. May be an absolute path or a path relative
        /// to the manifest's directory; callers are responsible for resolution.
        /// </summary>
        public string LibraryPath { get; }

        /// <summary>
        /// Returns <c>true</c> if the given parsed TOML content looks like a
        /// driver manifest rather than a connection profile.
        /// </summary>
        /// <remarks>
        /// The discriminator is the presence of <c>manifest_version</c> at the
        /// root level, or any <c>[Driver]</c> / <c>[Driver.shared]</c> section.
        /// </remarks>
        internal static bool LooksLikeManifest(Dictionary<string, Dictionary<string, object>> sections)
        {
            if (sections == null) return false;

            if (sections.TryGetValue("", out Dictionary<string, object>? root) &&
                root.ContainsKey(ManifestVersionField))
            {
                return true;
            }

            return sections.ContainsKey("Driver") || sections.ContainsKey("Driver.shared");
        }

        /// <summary>
        /// Parses a driver manifest from TOML content.
        /// </summary>
        /// <param name="tomlContent">The raw TOML text to parse.</param>
        /// <returns>The parsed <see cref="DriverManifest"/>.</returns>
        /// <exception cref="ArgumentNullException">If <paramref name="tomlContent"/> is null.</exception>
        /// <exception cref="AdbcException">
        /// Thrown when the TOML is malformed, the manifest version is unsupported,
        /// or no library path can be resolved for the current platform.
        /// </exception>
        public static DriverManifest LoadFromContent(string tomlContent)
        {
            if (tomlContent == null) throw new ArgumentNullException(nameof(tomlContent));

            Dictionary<string, Dictionary<string, object>> sections;
            try
            {
                sections = TomlParser.Parse(tomlContent);
            }
            catch (FormatException ex)
            {
                throw new AdbcException(
                    "Invalid TOML driver manifest: " + ex.Message,
                    AdbcStatusCode.InvalidArgument,
                    ex);
            }

            Dictionary<string, object> root = sections.TryGetValue("", out Dictionary<string, object>? r)
                ? r
                : new Dictionary<string, object>();

            long manifestVersion = ReadManifestVersion(root);
            if (manifestVersion != 1)
            {
                throw new AdbcException(
                    $"Driver manifest version '{manifestVersion}' is not supported by this driver manager.",
                    AdbcStatusCode.NotImplemented);
            }

            string? name = ReadOptionalString(root, "name");
            string? version = ReadOptionalString(root, "version");
            string? publisher = ReadOptionalString(root, "publisher");
            string? license = ReadOptionalString(root, "license");
            string? source = ReadOptionalString(root, "source");

            string? entrypoint = null;
            if (sections.TryGetValue("Driver", out Dictionary<string, object>? driverSection))
            {
                entrypoint = ReadOptionalString(driverSection, "entrypoint");
            }

            string libraryPath = ResolveLibraryPath(sections, driverSection);

            return new DriverManifest(
                manifestVersion,
                name,
                version,
                publisher,
                license,
                source,
                entrypoint,
                libraryPath);
        }

        /// <summary>
        /// Parses a driver manifest from a file path.
        /// </summary>
        public static DriverManifest LoadFromFile(string filePath)
        {
            if (filePath == null) throw new ArgumentNullException(nameof(filePath));
            string content = File.ReadAllText(filePath, Encoding.UTF8);
            return LoadFromContent(content);
        }

        private static long ReadManifestVersion(Dictionary<string, object> root)
        {
            // Per the spec, manifest_version defaults to 1 when absent.
            if (!root.TryGetValue(ManifestVersionField, out object? versionObj))
            {
                return 1;
            }

            if (versionObj is long lv)
            {
                return lv;
            }

            throw new AdbcException(
                $"The 'manifest_version' field has an invalid value '{versionObj}'. It must be an integer.",
                AdbcStatusCode.InvalidArgument);
        }

        private static string? ReadOptionalString(Dictionary<string, object> section, string key)
        {
            if (section.TryGetValue(key, out object? obj) && obj is string s)
            {
                return s;
            }
            return null;
        }

        /// <summary>
        /// Resolves the library path from the manifest. Supports the two spec forms:
        /// <c>[Driver].shared = "..."</c> (single string) and
        /// <c>[Driver.shared]</c> table keyed by platform tuple.
        /// </summary>
        private static string ResolveLibraryPath(
            Dictionary<string, Dictionary<string, object>> sections,
            Dictionary<string, object>? driverSection)
        {
            // Form 1: [Driver].shared = "/path/to/lib"
            if (driverSection != null &&
                driverSection.TryGetValue("shared", out object? sharedObj) &&
                sharedObj is string sharedPath)
            {
                if (string.IsNullOrEmpty(sharedPath))
                {
                    throw new AdbcException(
                        "Driver manifest has an empty 'Driver.shared' path.",
                        AdbcStatusCode.InvalidArgument);
                }
                return sharedPath;
            }

            // Form 2: [Driver.shared] table keyed by platform tuple
            if (sections.TryGetValue("Driver.shared", out Dictionary<string, object>? platformTable))
            {
                string current = GetCurrentPlatformTuple();
                if (platformTable.TryGetValue(current, out object? platObj) && platObj is string platPath)
                {
                    if (string.IsNullOrEmpty(platPath))
                    {
                        throw new AdbcException(
                            $"Driver manifest has an empty path for current platform '{current}'.",
                            AdbcStatusCode.InvalidArgument);
                    }
                    return platPath;
                }

                List<string> tuples = new List<string>(platformTable.Count);
                foreach (KeyValuePair<string, object> kv in platformTable) tuples.Add(kv.Key);
                tuples.Sort(StringComparer.Ordinal);
                throw new AdbcException(
                    $"Driver manifest has no entry for current platform '{current}'. " +
                    $"Available platforms: {string.Join(", ", tuples)}.",
                    AdbcStatusCode.NotFound);
            }

            throw new AdbcException(
                "Driver manifest does not specify a library path. " +
                "Provide a 'Driver.shared' value, either as a single string or a " +
                "platform-tuple-keyed table.",
                AdbcStatusCode.InvalidArgument);
        }

        /// <summary>
        /// Returns the platform tuple identifying the current OS and architecture,
        /// matching the format used by the ADBC driver-manifest spec
        /// (e.g. <c>windows_amd64</c>, <c>linux_arm64</c>, <c>macos_amd64</c>).
        /// </summary>
        internal static string GetCurrentPlatformTuple()
        {
            string os;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) os = "windows";
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) os = "macos";
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) os = "linux";
#if NET6_0_OR_GREATER
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.FreeBSD)) os = "freebsd";
#endif
            else os = "unknown";

            string arch;
            switch (RuntimeInformation.ProcessArchitecture)
            {
                case Architecture.X64: arch = "amd64"; break;
                case Architecture.Arm64: arch = "arm64"; break;
                case Architecture.X86: arch = "x86"; break;
                case Architecture.Arm: arch = "arm"; break;
                default: arch = RuntimeInformation.ProcessArchitecture.ToString().ToLowerInvariant(); break;
            }

            return os + "_" + arch;
        }
    }
}
