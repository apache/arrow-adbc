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
    /// Provides security-related functionality for the ADBC driver manager,
    /// including path validation, allowlist management, and audit logging.
    /// </summary>
    public static class DriverManagerSecurity
    {
        private static readonly object s_lock = new object();
        private static IDriverLoadAuditLogger? s_auditLogger;
        private static IDriverAllowlist? s_allowlist;

        /// <summary>
        /// Gets or sets the audit logger for driver loading operations.
        /// When set, all driver load attempts will be logged.
        /// </summary>
        /// <remarks>
        /// This property is thread-safe. Set to <c>null</c> to disable audit logging.
        /// </remarks>
        public static IDriverLoadAuditLogger? AuditLogger
        {
            get
            {
                lock (s_lock)
                {
                    return s_auditLogger;
                }
            }
            set
            {
                lock (s_lock)
                {
                    s_auditLogger = value;
                }
            }
        }

        /// <summary>
        /// Gets or sets the driver allowlist. When set, only drivers matching
        /// the allowlist criteria will be permitted to load.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This property is thread-safe. Set to <c>null</c> to disable allowlist
        /// validation (all drivers will be permitted).
        /// </para>
        /// <para>
        /// <b>Security Note:</b> In production environments, consider using an
        /// allowlist to restrict which drivers can be loaded, preventing
        /// potential arbitrary code execution attacks.
        /// </para>
        /// </remarks>
        public static IDriverAllowlist? Allowlist
        {
            get
            {
                lock (s_lock)
                {
                    return s_allowlist;
                }
            }
            set
            {
                lock (s_lock)
                {
                    s_allowlist = value;
                }
            }
        }

        /// <summary>
        /// Validates that a path does not contain path traversal sequences
        /// that could escape intended directories.
        /// </summary>
        /// <param name="path">The path to validate.</param>
        /// <param name="paramName">The parameter name for error messages.</param>
        /// <exception cref="AdbcException">
        /// Thrown when the path contains potentially dangerous sequences.
        /// </exception>
        public static void ValidatePathSecurity(string path, string paramName)
        {
            if (string.IsNullOrEmpty(path))
            {
                return;
            }

            // Check for path traversal attempts
            if (path.Contains(".."))
            {
                throw new AdbcException(
                    $"Path '{paramName}' contains potentially dangerous path traversal sequences (..).",
                    AdbcStatusCode.InvalidArgument);
            }

            // Check for null bytes (can be used to truncate paths in some systems)
            if (path.Contains("\0"))
            {
                throw new AdbcException(
                    $"Path '{paramName}' contains invalid null characters.",
                    AdbcStatusCode.InvalidArgument);
            }
        }

        /// <summary>
        /// Validates that a relative path in a manifest does not attempt to
        /// escape the manifest's directory.
        /// </summary>
        /// <param name="manifestDirectory">The directory containing the manifest.</param>
        /// <param name="relativePath">The relative path specified in the manifest.</param>
        /// <returns>The validated, canonicalized full path.</returns>
        /// <exception cref="AdbcException">
        /// Thrown when the resolved path escapes the manifest directory.
        /// </exception>
        public static string ValidateAndResolveManifestPath(string manifestDirectory, string relativePath)
        {
            ValidatePathSecurity(relativePath, "manifest driver path");

            // Resolve the full path
            string fullPath = Path.GetFullPath(Path.Combine(manifestDirectory, relativePath));
            string canonicalManifestDir = Path.GetFullPath(manifestDirectory);

            // Ensure the resolved path is within or below the manifest directory
            // This prevents path traversal attacks via symbolic links or other means
            if (!fullPath.StartsWith(canonicalManifestDir, StringComparison.OrdinalIgnoreCase))
            {
                throw new AdbcException(
                    $"Manifest driver path resolves outside the manifest directory. " +
                    $"This may indicate a path traversal attack.",
                    AdbcStatusCode.InvalidArgument);
            }

            return fullPath;
        }

        /// <summary>
        /// Logs a driver load attempt if an audit logger is configured.
        /// </summary>
        /// <param name="attempt">The driver load attempt details to log.</param>
        public static void LogDriverLoadAttempt(DriverLoadAttempt attempt)
        {
            IDriverLoadAuditLogger? logger = AuditLogger;
            if (logger != null)
            {
                try
                {
                    logger.LogDriverLoadAttempt(attempt);
                }
                catch
                {
                    // Don't let logging failures affect driver loading
                }
            }
        }

        /// <summary>
        /// Checks if a driver is allowed to be loaded based on the configured allowlist.
        /// </summary>
        /// <param name="driverPath">The path to the driver.</param>
        /// <param name="typeName">The type name for managed drivers, or null for native drivers.</param>
        /// <exception cref="AdbcException">
        /// Thrown when the driver is not permitted by the allowlist.
        /// </exception>
        public static void ValidateAllowlist(string driverPath, string? typeName)
        {
            IDriverAllowlist? allowlist = Allowlist;
            if (allowlist == null)
            {
                return; // No allowlist configured, all drivers permitted
            }

            if (!allowlist.IsDriverAllowed(driverPath, typeName))
            {
                string driverDescription = typeName != null
                    ? $"managed driver '{typeName}' from '{driverPath}'"
                    : $"native driver '{driverPath}'";

                throw new AdbcException(
                    $"Loading {driverDescription} is not permitted by the configured allowlist.",
                    AdbcStatusCode.Unauthorized);
            }
        }
    }

    /// <summary>
    /// Represents an attempt to load a driver, for audit logging purposes.
    /// </summary>
    public sealed class DriverLoadAttempt
    {
        /// <summary>
        /// Gets the UTC timestamp when the load attempt occurred.
        /// </summary>
        public DateTime TimestampUtc { get; }

        /// <summary>
        /// Gets the path to the driver being loaded.
        /// </summary>
        public string DriverPath { get; }

        /// <summary>
        /// Gets the type name for managed drivers, or null for native drivers.
        /// </summary>
        public string? TypeName { get; }

        /// <summary>
        /// Gets the manifest path if a manifest was used, or null otherwise.
        /// </summary>
        public string? ManifestPath { get; }

        /// <summary>
        /// Gets a value indicating whether the load attempt succeeded.
        /// </summary>
        public bool Success { get; }

        /// <summary>
        /// Gets the error message if the load attempt failed, or null if it succeeded.
        /// </summary>
        public string? ErrorMessage { get; }

        /// <summary>
        /// Gets the method used to load the driver (e.g., "LoadDriver", "LoadManagedDriver", "FindLoadDriver").
        /// </summary>
        public string LoadMethod { get; }

        /// <summary>
        /// Initializes a new instance of <see cref="DriverLoadAttempt"/>.
        /// </summary>
        public DriverLoadAttempt(
            string driverPath,
            string? typeName,
            string? manifestPath,
            bool success,
            string? errorMessage,
            string loadMethod)
        {
            TimestampUtc = DateTime.UtcNow;
            DriverPath = driverPath ?? throw new ArgumentNullException(nameof(driverPath));
            TypeName = typeName;
            ManifestPath = manifestPath;
            Success = success;
            ErrorMessage = errorMessage;
            LoadMethod = loadMethod ?? throw new ArgumentNullException(nameof(loadMethod));
        }
    }

    /// <summary>
    /// Interface for audit logging of driver load attempts.
    /// </summary>
    /// <remarks>
    /// Implement this interface to receive notifications about all driver
    /// loading operations for security monitoring and compliance purposes.
    /// </remarks>
    public interface IDriverLoadAuditLogger
    {
        /// <summary>
        /// Logs a driver load attempt.
        /// </summary>
        /// <param name="attempt">Details about the load attempt.</param>
        void LogDriverLoadAttempt(DriverLoadAttempt attempt);
    }

    /// <summary>
    /// Interface for validating whether a driver is allowed to be loaded.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Implement this interface to restrict which drivers can be loaded.
    /// This is useful for preventing arbitrary code execution attacks where
    /// a malicious manifest could redirect to a malicious driver.
    /// </para>
    /// <para>
    /// Example implementations:
    /// <list type="bullet">
    ///   <item>Allow only drivers from specific directories</item>
    ///   <item>Allow only drivers with specific file names</item>
    ///   <item>Allow only signed assemblies</item>
    ///   <item>Allow only specific type names for managed drivers</item>
    /// </list>
    /// </para>
    /// </remarks>
    public interface IDriverAllowlist
    {
        /// <summary>
        /// Determines whether a driver is allowed to be loaded.
        /// </summary>
        /// <param name="driverPath">The full path to the driver file.</param>
        /// <param name="typeName">
        /// For managed drivers, the fully-qualified type name. For native drivers, null.
        /// </param>
        /// <returns>
        /// <c>true</c> if the driver is allowed to be loaded; otherwise, <c>false</c>.
        /// </returns>
        bool IsDriverAllowed(string driverPath, string? typeName);
    }

    /// <summary>
    /// A simple directory-based driver allowlist that permits drivers
    /// only from specific directories.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This allowlist implementation checks whether the driver path starts
    /// with one of the allowed directory paths. It uses case-insensitive
    /// comparison on Windows and case-sensitive on other platforms.
    /// </para>
    /// <para>
    /// <b>Example usage:</b>
    /// <code>
    /// var allowlist = new DirectoryAllowlist(new[]
    /// {
    ///     @"C:\Program Files\ADBC\Drivers",
    ///     @"C:\MyApp\Drivers"
    /// });
    /// DriverManagerSecurity.Allowlist = allowlist;
    /// </code>
    /// </para>
    /// </remarks>
    public sealed class DirectoryAllowlist : IDriverAllowlist
    {
        private readonly List<string> _allowedDirectories;
        private readonly StringComparison _comparison;

        /// <summary>
        /// Initializes a new instance of <see cref="DirectoryAllowlist"/>.
        /// </summary>
        /// <param name="allowedDirectories">
        /// The directories from which drivers may be loaded. Paths are
        /// canonicalized during construction.
        /// </param>
        public DirectoryAllowlist(IEnumerable<string> allowedDirectories)
        {
            if (allowedDirectories == null)
            {
                throw new ArgumentNullException(nameof(allowedDirectories));
            }

            _allowedDirectories = new List<string>();
            foreach (string dir in allowedDirectories)
            {
                if (!string.IsNullOrWhiteSpace(dir))
                {
                    // Canonicalize and ensure trailing separator for proper prefix matching
                    string canonical = Path.GetFullPath(dir);
                    if (!canonical.EndsWith(Path.DirectorySeparatorChar.ToString()))
                    {
                        canonical += Path.DirectorySeparatorChar;
                    }
                    _allowedDirectories.Add(canonical);
                }
            }

            // Use case-insensitive comparison on Windows
            _comparison = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal;
        }

        /// <inheritdoc/>
        public bool IsDriverAllowed(string driverPath, string? typeName)
        {
            if (string.IsNullOrEmpty(driverPath))
            {
                return false;
            }

            string canonicalPath = Path.GetFullPath(driverPath);

            foreach (string allowedDir in _allowedDirectories)
            {
                if (canonicalPath.StartsWith(allowedDir, _comparison))
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>
    /// A type-based driver allowlist that permits only specific driver types
    /// for managed drivers.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This allowlist implementation checks whether the driver type name
    /// matches one of the explicitly allowed type names. For native drivers,
    /// it delegates to an optional inner allowlist or permits all native drivers.
    /// </para>
    /// <para>
    /// <b>Example usage:</b>
    /// <code>
    /// var allowlist = new TypeAllowlist(new[]
    /// {
    ///     "Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver",
    ///     "Apache.Arrow.Adbc.Drivers.Snowflake.SnowflakeDriver"
    /// });
    /// DriverManagerSecurity.Allowlist = allowlist;
    /// </code>
    /// </para>
    /// </remarks>
    public sealed class TypeAllowlist : IDriverAllowlist
    {
        private readonly HashSet<string> _allowedTypes;
        private readonly IDriverAllowlist? _nativeDriverAllowlist;

        /// <summary>
        /// Initializes a new instance of <see cref="TypeAllowlist"/>.
        /// </summary>
        /// <param name="allowedTypeNames">
        /// The fully-qualified type names that are allowed for managed drivers.
        /// </param>
        /// <param name="nativeDriverAllowlist">
        /// Optional allowlist for native drivers. If null, all native drivers are permitted.
        /// </param>
        public TypeAllowlist(
            IEnumerable<string> allowedTypeNames,
            IDriverAllowlist? nativeDriverAllowlist = null)
        {
            if (allowedTypeNames == null)
            {
                throw new ArgumentNullException(nameof(allowedTypeNames));
            }

            _allowedTypes = new HashSet<string>(allowedTypeNames, StringComparer.Ordinal);
            _nativeDriverAllowlist = nativeDriverAllowlist;
        }

        /// <inheritdoc/>
        public bool IsDriverAllowed(string driverPath, string? typeName)
        {
            if (typeName != null)
            {
                // Managed driver - check type allowlist
                return _allowedTypes.Contains(typeName);
            }
            else
            {
                // Native driver - delegate to inner allowlist or permit
                return _nativeDriverAllowlist?.IsDriverAllowed(driverPath, null) ?? true;
            }
        }
    }

    /// <summary>
    /// Combines multiple allowlists, requiring all of them to permit a driver.
    /// </summary>
    public sealed class CompositeAllowlist : IDriverAllowlist
    {
        private readonly IDriverAllowlist[] _allowlists;

        /// <summary>
        /// Initializes a new instance of <see cref="CompositeAllowlist"/>.
        /// </summary>
        /// <param name="allowlists">
        /// The allowlists to combine. All must return true for a driver to be permitted.
        /// </param>
        public CompositeAllowlist(params IDriverAllowlist[] allowlists)
        {
            _allowlists = allowlists ?? throw new ArgumentNullException(nameof(allowlists));
        }

        /// <inheritdoc/>
        public bool IsDriverAllowed(string driverPath, string? typeName)
        {
            foreach (IDriverAllowlist allowlist in _allowlists)
            {
                if (!allowlist.IsDriverAllowed(driverPath, typeName))
                {
                    return false;
                }
            }
            return true;
        }
    }
}
