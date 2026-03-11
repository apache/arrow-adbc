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
using System.Reflection;
using System.Runtime.InteropServices;
using Apache.Arrow.Adbc.C;

namespace Apache.Arrow.Adbc.DriverManager
{
    /// <summary>
    /// Provides methods to locate and load ADBC drivers, optionally using TOML manifest
    /// files and connection profiles.
    /// Mirrors the free functions declared in <c>adbc_driver_manager.h</c>:
    /// <c>AdbcLoadDriver</c> and <c>AdbcFindLoadDriver</c>.
    /// </summary>
    public static class AdbcDriverManager
    {
        /// <summary>
        /// The environment variable that specifies additional driver search paths.
        /// </summary>
        public const string DriverPathEnvVar = "ADBC_DRIVER_PATH";

        private static readonly string[] s_nativeExtensions = { ".so", ".dll", ".dylib" };

        // -----------------------------------------------------------------------
        // AdbcLoadDriver – load a driver directly from an absolute or relative path
        // -----------------------------------------------------------------------

        /// <summary>
        /// Loads an ADBC driver from an explicit file path.
        /// Mirrors <c>AdbcLoadDriver</c> in <c>adbc_driver_manager.h</c>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If a TOML manifest file with the same base name exists in the same directory
        /// as the driver file, it will be automatically detected and used. For example,
        /// loading <c>libadbc_driver_snowflake.dll</c> will automatically use
        /// <c>libadbc_driver_snowflake.toml</c> if present.
        /// </para>
        /// <para>
        /// The co-located manifest can specify default options, override the driver path,
        /// or provide additional metadata. The <paramref name="entrypoint"/> parameter
        /// always takes precedence over any entrypoint derived from the manifest or filename.
        /// </para>
        /// </remarks>
        /// <param name="driverPath">
        /// The absolute or relative path to the driver shared library.
        /// </param>
        /// <param name="entrypoint">
        /// The symbol name of the driver initialisation function. When <c>null</c> the
        /// driver manager derives a candidate entrypoint from the file name (see
        /// <see cref="DeriveEntrypoint"/>), falling back to <c>AdbcDriverInit</c>.
        /// </param>
        /// <returns>The loaded <see cref="AdbcDriver"/>.</returns>
        public static AdbcDriver LoadDriver(string driverPath, string? entrypoint = null)
        {
            if (string.IsNullOrEmpty(driverPath))
                throw new ArgumentException("Driver path must not be null or empty.", nameof(driverPath));

            // Check for co-located TOML manifest
            string? colocatedManifest = TryFindColocatedManifest(driverPath);
            if (colocatedManifest != null)
            {
                return LoadFromManifest(colocatedManifest, entrypoint);
            }

            string resolvedEntrypoint = entrypoint ?? DeriveEntrypoint(driverPath);
            return CAdbcDriverImporter.Load(driverPath, resolvedEntrypoint);
        }

        // -----------------------------------------------------------------------
        // AdbcFindLoadDriver – locate a driver by name using configurable search rules
        // -----------------------------------------------------------------------

        /// <summary>
        /// Searches for an ADBC driver by name and loads it.
        /// Mirrors <c>AdbcFindLoadDriver</c> in <c>adbc_driver_manager.h</c>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If <paramref name="driverName"/> is an absolute path:
        /// <list type="bullet">
        ///   <item>A <c>.toml</c> extension triggers manifest loading.</item>
        ///   <item>Any other extension loads the path as a shared library directly.</item>
        /// </list>
        /// </para>
        /// <para>
        /// If <paramref name="driverName"/> is a bare name (no extension, not absolute):
        /// <list type="number">
        ///   <item>Each configured search directory is checked in order.</item>
        ///   <item>
        ///     For each directory, <c>&lt;dir&gt;/&lt;name&gt;.toml</c> is attempted first;
        ///     if found the manifest is parsed and the driver loaded.
        ///   </item>
        ///   <item>
        ///     Then <c>&lt;dir&gt;/&lt;name&gt;.&lt;ext&gt;</c> for each platform extension
        ///     (<c>.dll</c>, <c>.so</c>, <c>.dylib</c>) is attempted.
        ///   </item>
        /// </list>
        /// </para>
        /// </remarks>
        /// <param name="driverName">
        /// A driver identifier: an absolute path (with or without extension) or a bare
        /// driver name to be resolved by directory search.
        /// </param>
        /// <param name="entrypoint">
        /// The symbol name of the driver initialisation function, or <c>null</c> to
        /// derive it automatically.
        /// </param>
        /// <param name="loadOptions">
        /// Flags controlling which directories are searched.
        /// </param>
        /// <param name="additionalSearchPathList">
        /// An optional OS path-list-separator-delimited list of extra directories to
        /// search before the standard ones, or <c>null</c>.
        /// </param>
        /// <returns>The loaded <see cref="AdbcDriver"/>.</returns>
        /// <exception cref="AdbcException">Thrown when the driver cannot be found or loaded.</exception>
        public static AdbcDriver FindLoadDriver(
            string driverName,
            string? entrypoint = null,
            AdbcLoadFlags loadOptions = AdbcLoadFlags.Default,
            string? additionalSearchPathList = null)
        {
            if (string.IsNullOrEmpty(driverName))
                throw new ArgumentException("Driver name must not be null or empty.", nameof(driverName));

            // Absolute path – load directly.
            if (Path.IsPathRooted(driverName))
                return LoadFromAbsolutePath(driverName, entrypoint);

            // Bare name with an extension but not rooted – relative path case.
            string ext = Path.GetExtension(driverName);
            if (!string.IsNullOrEmpty(ext) && loadOptions.HasFlag(AdbcLoadFlags.AllowRelativePaths))
            {
                if (string.Equals(ext, ".toml", StringComparison.OrdinalIgnoreCase))
                    return LoadFromManifest(driverName, entrypoint);
                return LoadDriver(driverName, entrypoint);
            }

            // Bareword – search configured directories.
            foreach (string dir in BuildSearchDirectories(loadOptions, additionalSearchPathList))
            {
                AdbcDriver? found = TryLoadFromDirectory(dir, driverName, entrypoint);
                if (found != null)
                    return found;
            }

            throw new AdbcException(
                $"Could not find ADBC driver '{driverName}' in any configured search path.",
                AdbcStatusCode.NotFound);
        }

        // -----------------------------------------------------------------------
        // LoadDriverFromProfile – load a driver as specified by a connection profile
        // -----------------------------------------------------------------------

        /// <summary>
        /// Loads an ADBC driver as specified by an <see cref="IConnectionProfile"/>.
        /// </summary>
        /// <param name="profile">The profile that specifies the driver to load.</param>
        /// <param name="entrypoint">
        /// An optional override for the driver entrypoint symbol. When <c>null</c> the
        /// entrypoint is derived from the driver name in the profile.
        /// </param>
        /// <param name="loadOptions">Flags controlling directory search behaviour.</param>
        /// <param name="additionalSearchPathList">
        /// An optional additional search path list, overriding any search paths that may
        /// be embedded in the profile.
        /// </param>
        /// <returns>The loaded <see cref="AdbcDriver"/>.</returns>
        /// <exception cref="AdbcException">
        /// Thrown when the profile does not specify a driver, or the driver cannot be found.
        /// </exception>
        public static AdbcDriver LoadDriverFromProfile(
            IConnectionProfile profile,
            string? entrypoint = null,
            AdbcLoadFlags loadOptions = AdbcLoadFlags.Default,
            string? additionalSearchPathList = null)
        {
            if (profile == null) throw new ArgumentNullException(nameof(profile));

            if (string.IsNullOrEmpty(profile.DriverName))
                throw new AdbcException(
                    "The connection profile does not specify a driver name.",
                    AdbcStatusCode.InvalidArgument);

            return FindLoadDriver(profile.DriverName!, entrypoint, loadOptions, additionalSearchPathList);
        }

        // -----------------------------------------------------------------------
        // LoadManagedDriver – load a managed .NET AdbcDriver via reflection
        // -----------------------------------------------------------------------

        /// <summary>
        /// Loads a managed (pure .NET) ADBC driver from a .NET assembly using reflection.
        /// </summary>
        /// <remarks>
        /// Use this instead of <see cref="LoadDriver"/> when the driver is a .NET assembly
        /// (e.g. <c>Apache.Arrow.Adbc.Drivers.BigQuery.dll</c>) rather than a native shared
        /// library. The assembly is loaded via <see cref="Assembly.LoadFrom"/> and the
        /// specified type is instantiated with a public parameterless constructor.
        /// </remarks>
        /// <param name="assemblyPath">
        /// The absolute or relative path to the .NET assembly containing the driver.
        /// </param>
        /// <param name="typeName">
        /// The fully-qualified name of a concrete class that derives from
        /// <see cref="AdbcDriver"/> and has a public parameterless constructor
        /// (e.g. <c>Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver</c>).
        /// </param>
        /// <returns>An instance of the specified <see cref="AdbcDriver"/> subclass.</returns>
        /// <exception cref="AdbcException">
        /// Thrown when the assembly cannot be loaded, the type is not found, or the type
        /// does not derive from <see cref="AdbcDriver"/>.
        /// </exception>
        public static AdbcDriver LoadManagedDriver(string assemblyPath, string typeName)
        {
            if (string.IsNullOrEmpty(assemblyPath))
                throw new ArgumentException("Assembly path must not be null or empty.", nameof(assemblyPath));
            if (string.IsNullOrEmpty(typeName))
                throw new ArgumentException("Type name must not be null or empty.", nameof(typeName));

            Assembly assembly;
            try
            {
                assembly = Assembly.LoadFrom(assemblyPath);
            }
            catch (Exception ex)
            {
                throw new AdbcException(
                    $"Failed to load managed driver assembly '{assemblyPath}': {ex.Message}",
                    AdbcStatusCode.IOError);
            }

            Type? driverType = assembly.GetType(typeName, throwOnError: false);
            if (driverType == null)
                throw new AdbcException(
                    $"Type '{typeName}' was not found in assembly '{assemblyPath}'.",
                    AdbcStatusCode.NotFound);

            if (!typeof(AdbcDriver).IsAssignableFrom(driverType))
                throw new AdbcException(
                    $"Type '{typeName}' does not derive from {nameof(AdbcDriver)}.",
                    AdbcStatusCode.InvalidArgument);

            object? instance;
            try
            {
                instance = Activator.CreateInstance(driverType);
            }
            catch (Exception ex)
            {
                throw new AdbcException(
                    $"Failed to instantiate driver type '{typeName}': {ex.Message}",
                    AdbcStatusCode.InternalError);
            }

            if (instance == null)
                throw new AdbcException(
                    $"Activator returned null for driver type '{typeName}'.",
                    AdbcStatusCode.InternalError);

            return (AdbcDriver)instance;
        }

        // -----------------------------------------------------------------------
        // OpenDatabaseFromProfile – load driver + open database in one step
        // -----------------------------------------------------------------------

        /// <summary>
        /// Loads the driver specified by <paramref name="profile"/> and opens a database,
        /// applying all options from the profile as connection parameters.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If the profile has a non-null <see cref="IConnectionProfile.DriverTypeName"/>, the
        /// driver is loaded as a managed .NET assembly via <see cref="LoadManagedDriver"/> and
        /// <see cref="IConnectionProfile.DriverName"/> is used as the assembly path.
        /// </para>
        /// <para>
        /// Otherwise the driver is loaded as a native shared library via
        /// <see cref="FindLoadDriver"/>.
        /// </para>
        /// <para>
        /// All options (string, integer, and double) are merged into a single
        /// <c>string → string</c> dictionary.  Integer and double values are formatted
        /// using <see cref="CultureInfo.InvariantCulture"/>. The merged dictionary is
        /// passed to <see cref="AdbcDriver.Open(IReadOnlyDictionary{string,string})"/>.
        /// </para>
        /// <para>
        /// Call <see cref="TomlConnectionProfile.ResolveEnvVars"/> on the profile before
        /// passing it here if you want <c>env_var(NAME)</c> values expanded first.
        /// </para>
        /// </remarks>
        /// <param name="profile">The connection profile specifying the driver and options.</param>
        /// <param name="loadOptions">Flags controlling directory search for native drivers.</param>
        /// <param name="additionalSearchPathList">
        /// An optional extra search path list for native driver discovery.
        /// </param>
        /// <returns>An open <see cref="AdbcDatabase"/>.</returns>
        public static AdbcDatabase OpenDatabaseFromProfile(
            IConnectionProfile profile,
            AdbcLoadFlags loadOptions = AdbcLoadFlags.Default,
            string? additionalSearchPathList = null)
        {
            return OpenDatabaseFromProfile(profile, null, loadOptions, additionalSearchPathList);
        }

        /// <summary>
        /// Loads the driver specified by <paramref name="profile"/> and opens a database,
        /// merging profile options with explicitly provided options.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This overload allows specifying additional options that will be merged with the
        /// profile options. Per the ADBC specification, profile options are applied first,
        /// then explicit options. If the same key appears in both the profile and explicit
        /// options, the explicit value takes precedence.
        /// </para>
        /// <para>
        /// If the profile has a non-null <see cref="IConnectionProfile.DriverTypeName"/>, the
        /// driver is loaded as a managed .NET assembly via <see cref="LoadManagedDriver"/> and
        /// <see cref="IConnectionProfile.DriverName"/> is used as the assembly path.
        /// </para>
        /// <para>
        /// Otherwise the driver is loaded as a native shared library via
        /// <see cref="FindLoadDriver"/>.
        /// </para>
        /// <para>
        /// All options are merged into a single <c>string → string</c> dictionary in the
        /// following order (later values override earlier ones for the same key):
        /// <list type="number">
        ///   <item>Profile integer options (formatted as strings)</item>
        ///   <item>Profile double options (formatted as strings)</item>
        ///   <item>Profile string options</item>
        ///   <item>Explicit options from <paramref name="explicitOptions"/></item>
        /// </list>
        /// The merged dictionary is passed to <see cref="AdbcDriver.Open(IReadOnlyDictionary{string,string})"/>.
        /// </para>
        /// </remarks>
        /// <param name="profile">The connection profile specifying the driver and options.</param>
        /// <param name="explicitOptions">
        /// Additional options to merge with profile options. Explicit options override profile
        /// options for the same key. May be <c>null</c> or empty.
        /// </param>
        /// <param name="loadOptions">Flags controlling directory search for native drivers.</param>
        /// <param name="additionalSearchPathList">
        /// An optional extra search path list for native driver discovery.
        /// </param>
        /// <returns>An open <see cref="AdbcDatabase"/>.</returns>
        public static AdbcDatabase OpenDatabaseFromProfile(
            IConnectionProfile profile,
            IReadOnlyDictionary<string, string>? explicitOptions,
            AdbcLoadFlags loadOptions = AdbcLoadFlags.Default,
            string? additionalSearchPathList = null)
        {
            if (profile == null) throw new ArgumentNullException(nameof(profile));

            AdbcDriver driver;

            if (!string.IsNullOrEmpty(profile.DriverTypeName))
            {
                // Managed .NET driver path
                if (string.IsNullOrEmpty(profile.DriverName))
                    throw new AdbcException(
                        "The connection profile specifies a driver_type but no driver assembly path (driver field).",
                        AdbcStatusCode.InvalidArgument);

                driver = LoadManagedDriver(profile.DriverName!, profile.DriverTypeName!);
            }
            else
            {
                // Native shared-library path
                driver = LoadDriverFromProfile(profile, null, loadOptions, additionalSearchPathList);
            }

            return driver.Open(BuildStringOptions(profile, explicitOptions));
        }

        /// <summary>
        /// Merges all options from a profile into a flat <c>string → string</c> dictionary
        /// suitable for passing to <see cref="AdbcDriver.Open(IReadOnlyDictionary{string,string})"/>.
        /// Integer and double values are formatted with <see cref="CultureInfo.InvariantCulture"/>.
        /// String options take precedence if the same key appears in multiple option sets.
        /// </summary>
        public static IReadOnlyDictionary<string, string> BuildStringOptions(IConnectionProfile profile)
        {
            return BuildStringOptions(profile, null);
        }

        /// <summary>
        /// Merges options from a profile with explicitly provided options into a flat
        /// <c>string → string</c> dictionary suitable for passing to
        /// <see cref="AdbcDriver.Open(IReadOnlyDictionary{string,string})"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Options are merged in the following order (later values override earlier ones):
        /// <list type="number">
        ///   <item>Profile integer options (formatted as strings)</item>
        ///   <item>Profile double options (formatted as strings)</item>
        ///   <item>Profile string options</item>
        ///   <item>Explicit options from <paramref name="explicitOptions"/></item>
        /// </list>
        /// </para>
        /// <para>
        /// This ordering ensures that explicit options always take precedence over profile
        /// options, as required by the ADBC specification.
        /// </para>
        /// </remarks>
        /// <param name="profile">The connection profile containing base options.</param>
        /// <param name="explicitOptions">
        /// Additional options that override profile options. May be <c>null</c> or empty.
        /// </param>
        /// <returns>A merged dictionary of all options.</returns>
        public static IReadOnlyDictionary<string, string> BuildStringOptions(
            IConnectionProfile profile,
            IReadOnlyDictionary<string, string>? explicitOptions)
        {
            if (profile == null) throw new ArgumentNullException(nameof(profile));

            var merged = new Dictionary<string, string>(StringComparer.Ordinal);

            // Profile options first (in order: int, double, string)
            foreach (var kv in profile.IntOptions)
                merged[kv.Key] = kv.Value.ToString(CultureInfo.InvariantCulture);

            foreach (var kv in profile.DoubleOptions)
                merged[kv.Key] = kv.Value.ToString(CultureInfo.InvariantCulture);

            foreach (var kv in profile.StringOptions)
                merged[kv.Key] = kv.Value;

            // Explicit options last – they override profile options
            if (explicitOptions != null)
            {
                foreach (var kv in explicitOptions)
                    merged[kv.Key] = kv.Value;
            }

            return merged;
        }

        // -----------------------------------------------------------------------
        // Helpers – derive entrypoint, search directories, manifest loading
        // -----------------------------------------------------------------------

        /// <summary>
        /// Derives a candidate entrypoint symbol name from a driver file path.
        /// </summary>
        /// <remarks>
        /// The convention used by the Go-based ADBC drivers is to strip leading
        /// <c>lib</c> and any extension, then append <c>Init</c>. For example,
        /// <c>libadbc_driver_postgresql.so</c> yields <c>AdbcDriverPostgresqlInit</c>.
        /// Falls back to <c>AdbcDriverInit</c> when no better candidate can be formed.
        /// </remarks>
        public static string DeriveEntrypoint(string driverPath)
        {
            string baseName = Path.GetFileNameWithoutExtension(driverPath);

            // Strip leading "lib" prefix common on POSIX platforms.
            if (baseName.StartsWith("lib", StringComparison.OrdinalIgnoreCase))
                baseName = baseName.Substring(3);

            // Strip "adbc_driver_" or "adbc_" prefix to get a shorter stem.
            const string adbcDriverPrefix = "adbc_driver_";
            const string adbcPrefix = "adbc_";

            if (baseName.StartsWith(adbcDriverPrefix, StringComparison.OrdinalIgnoreCase))
                baseName = baseName.Substring(adbcDriverPrefix.Length);
            else if (baseName.StartsWith(adbcPrefix, StringComparison.OrdinalIgnoreCase))
                baseName = baseName.Substring(adbcPrefix.Length);

            if (string.IsNullOrEmpty(baseName))
                return "AdbcDriverInit";

            // Convert snake_case to PascalCase.
            string pascal = ToPascalCase(baseName);
            return $"AdbcDriver{pascal}Init";
        }

        private static AdbcDriver LoadFromAbsolutePath(string path, string? entrypoint)
        {
            string ext = Path.GetExtension(path);
            if (string.Equals(ext, ".toml", StringComparison.OrdinalIgnoreCase))
                return LoadFromManifest(path, entrypoint);

            // Check for co-located TOML manifest (e.g., libadbc_driver_snowflake.toml
            // alongside libadbc_driver_snowflake.dll)
            string? colocatedManifest = TryFindColocatedManifest(path);
            if (colocatedManifest != null)
            {
                return LoadFromManifest(colocatedManifest, entrypoint);
            }

            return LoadDriver(path, entrypoint);
        }

        /// <summary>
        /// Checks for a TOML manifest file co-located with a driver file.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Given a driver path like <c>C:\drivers\libadbc_driver_snowflake.dll</c>,
        /// this checks for <c>C:\drivers\libadbc_driver_snowflake.toml</c>.
        /// </para>
        /// <para>
        /// Co-located manifests allow drivers to ship with metadata about how they should
        /// be loaded (e.g., specifying they're managed .NET drivers via <c>driver_type</c>,
        /// or redirecting to the actual driver location via the <c>driver</c> field).
        /// </para>
        /// <para>
        /// <b>Important:</b> Options specified in co-located manifests are NOT automatically
        /// applied to database connections. The manifest is used solely for driver loading.
        /// To use manifest options, explicitly load the profile with
        /// <see cref="TomlConnectionProfile.FromFile"/> and use
        /// <see cref="OpenDatabaseFromProfile(IConnectionProfile, IReadOnlyDictionary{string, string}?, AdbcLoadFlags, string?)"/>.
        /// </para>
        /// </remarks>
        /// <param name="driverPath">The path to the driver file.</param>
        /// <returns>
        /// The full path to the co-located manifest if found; otherwise <c>null</c>.
        /// </returns>
        private static string? TryFindColocatedManifest(string driverPath)
        {
            try
            {
                string fullPath = Path.GetFullPath(driverPath);
                string? directory = Path.GetDirectoryName(fullPath);
                if (string.IsNullOrEmpty(directory))
                    return null;

                string fileNameWithoutExt = Path.GetFileNameWithoutExtension(fullPath);
                string manifestPath = Path.Combine(directory, fileNameWithoutExt + ".toml");

                return File.Exists(manifestPath) ? manifestPath : null;
            }
            catch
            {
                // If path resolution fails, just return null
                return null;
            }
        }

        private static AdbcDriver LoadFromManifest(string manifestPath, string? entrypoint)
        {
            if (!File.Exists(manifestPath))
                throw new AdbcException(
                    $"Driver manifest file not found: '{manifestPath}'.",
                    AdbcStatusCode.NotFound);

            TomlConnectionProfile manifest = TomlConnectionProfile.FromFile(manifestPath);

            if (string.IsNullOrEmpty(manifest.DriverName))
                throw new AdbcException(
                    $"Driver manifest '{manifestPath}' does not specify a 'driver' field.",
                    AdbcStatusCode.InvalidArgument);

            // Check if this is a managed driver
            if (!string.IsNullOrEmpty(manifest.DriverTypeName))
            {
                // Managed .NET driver - resolve relative path relative to manifest directory
                string driverPath = manifest.DriverName!;
                if (!Path.IsPathRooted(driverPath))
                {
                    string? manifestDir = Path.GetDirectoryName(Path.GetFullPath(manifestPath));
                    if (!string.IsNullOrEmpty(manifestDir))
                        driverPath = Path.Combine(manifestDir, driverPath);
                }
                return LoadManagedDriver(driverPath, manifest.DriverTypeName!);
            }

            // Native driver - resolve entrypoint and path
            string resolvedEntrypoint = entrypoint ?? DeriveEntrypoint(manifest.DriverName!);

            // Resolve relative driver path relative to the manifest directory.
            string resolvedDriverPath = manifest.DriverName!;
            if (!Path.IsPathRooted(resolvedDriverPath))
            {
                string? manifestDir = Path.GetDirectoryName(Path.GetFullPath(manifestPath));
                if (!string.IsNullOrEmpty(manifestDir))
                    resolvedDriverPath = Path.Combine(manifestDir, resolvedDriverPath);
            }

            return CAdbcDriverImporter.Load(resolvedDriverPath, resolvedEntrypoint);
        }

        private static AdbcDriver? TryLoadFromDirectory(string dir, string driverName, string? entrypoint)
        {
            // 1. Try manifest
            string manifestPath = Path.Combine(dir, driverName + ".toml");
            if (File.Exists(manifestPath))
            {
                try { return LoadFromManifest(manifestPath, entrypoint); }
                catch (AdbcException) { throw; }   // surface manifest parse errors
            }

            // 2. Try native library extensions
            foreach (string nativeExt in GetPlatformExtensions())
            {
                string libPath = Path.Combine(dir, driverName + nativeExt);
                if (File.Exists(libPath))
                {
                    try
                    {
                        string ep = entrypoint ?? DeriveEntrypoint(libPath);
                        return CAdbcDriverImporter.Load(libPath, ep);
                    }
                    catch (AdbcException) { /* try next extension */ }
                }
            }

            return null;
        }

        private static IEnumerable<string> BuildSearchDirectories(
            AdbcLoadFlags flags,
            string? additionalSearchPathList)
        {
            // 1. Caller-supplied additional paths
            if (!string.IsNullOrEmpty(additionalSearchPathList))
            {
                foreach (string p in SplitPathList(additionalSearchPathList!))
                    yield return p;
            }

            // 2. ADBC_DRIVER_PATH environment variable
            if (flags.HasFlag(AdbcLoadFlags.SearchEnv))
            {
                string? envPath = Environment.GetEnvironmentVariable(DriverPathEnvVar);
                if (!string.IsNullOrEmpty(envPath))
                {
                    foreach (string p in SplitPathList(envPath!))
                        yield return p;
                }
            }

            // 3. User-level directory
            if (flags.HasFlag(AdbcLoadFlags.SearchUser))
            {
                string userDir = GetUserDriverDirectory();
                if (!string.IsNullOrEmpty(userDir))
                    yield return userDir;
            }

            // 4. System-level directory
            if (flags.HasFlag(AdbcLoadFlags.SearchSystem))
            {
                string sysDir = GetSystemDriverDirectory();
                if (!string.IsNullOrEmpty(sysDir))
                    yield return sysDir;
            }
        }

        private static string[] GetPlatformExtensions()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return new[] { ".dll" };
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                return new[] { ".dylib", ".so" };
            return new[] { ".so", ".dylib" };
        }

        private static string GetUserDriverDirectory()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                string? appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
                return string.IsNullOrEmpty(appData) ? string.Empty : Path.Combine(appData, "adbc", "drivers");
            }
            else
            {
                string home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
                return string.IsNullOrEmpty(home) ? string.Empty : Path.Combine(home, ".config", "adbc", "drivers");
            }
        }

        private static string GetSystemDriverDirectory()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                string? sysRoot = Environment.GetEnvironmentVariable("ProgramData");
                return string.IsNullOrEmpty(sysRoot) ? string.Empty : Path.Combine(sysRoot, "adbc", "drivers");
            }
            else
            {
                return "/usr/lib/adbc";
            }
        }

        private static string[] SplitPathList(string pathList) =>
            pathList.Split(new char[] { Path.PathSeparator }, StringSplitOptions.RemoveEmptyEntries);

        private static string ToPascalCase(string snake)
        {
            var sb = new System.Text.StringBuilder();
            bool upperNext = true;
            foreach (char c in snake)
            {
                if (c == '_')
                {
                    upperNext = true;
                }
                else if (upperNext)
                {
                    sb.Append(char.ToUpperInvariant(c));
                    upperNext = false;
                }
                else
                {
                    sb.Append(char.ToLowerInvariant(c));
                }
            }
            return sb.ToString();
        }
    }
}
