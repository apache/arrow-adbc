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
using Apache.Arrow.Adbc.DriverManager;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.DriverManager
{
    /// <summary>
    /// End-to-end proof that <see cref="DriverManagerSecurity.Allowlist"/> and
    /// <see cref="DriverManagerSecurity.AuditLogger"/> are actually consulted by
    /// <see cref="AdbcDriverManager"/> on the real driver-load hot path -- i.e.
    /// they are not "documented but non-functional" surface area.
    /// </summary>
    /// <remarks>
    /// Mutates process-wide static state on <see cref="DriverManagerSecurity"/>;
    /// xUnit's collection serialization keeps these tests off the parallel queue
    /// alongside <see cref="DriverManagerSecurityTests"/>.
    /// </remarks>
    [Collection("DriverManagerSecurity")]
    public sealed class DriverManagerSecurityIntegrationTests : IDisposable
    {
        private readonly List<string> _tempDirs = new List<string>();
        private readonly IDriverAllowlist? _originalAllowlist;
        private readonly IDriverLoadAuditLogger? _originalLogger;

        public DriverManagerSecurityIntegrationTests()
        {
            _originalAllowlist = DriverManagerSecurity.Allowlist;
            _originalLogger = DriverManagerSecurity.AuditLogger;
        }

        public void Dispose()
        {
            DriverManagerSecurity.Allowlist = _originalAllowlist;
            DriverManagerSecurity.AuditLogger = _originalLogger;
            foreach (string d in _tempDirs)
            {
                try { if (Directory.Exists(d)) Directory.Delete(d, true); } catch { }
            }
        }

        private string CreateTempDir()
        {
            string dir = Path.Combine(Path.GetTempPath(), "adbc_sec_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            _tempDirs.Add(dir);
            return dir;
        }

        private string CopyDriverAssembly(string destDir)
        {
            string sourcePath = typeof(FakeAdbcDriver).Assembly.Location;
            string destPath = Path.Combine(destDir, Path.GetFileName(sourcePath));
            File.Copy(sourcePath, destPath, overwrite: true);

            string sourceDir = Path.GetDirectoryName(sourcePath)!;
            string baseName = Path.GetFileNameWithoutExtension(sourcePath);
            foreach (string companion in new[] { ".deps.json", ".runtimeconfig.json" })
            {
                string from = Path.Combine(sourceDir, baseName + companion);
                if (File.Exists(from))
                {
                    File.Copy(from, Path.Combine(destDir, baseName + companion), overwrite: true);
                }
            }
            return destPath;
        }

        private sealed class CapturingLogger : IDriverLoadAuditLogger
        {
            private readonly object _gate = new object();
            private readonly List<DriverLoadAttempt> _attempts = new List<DriverLoadAttempt>();
            private readonly string? _driverPathFilter;

            public CapturingLogger(string? driverPathFilter = null)
            {
                _driverPathFilter = driverPathFilter;
            }

            // Snapshot under lock so concurrent test runs of unrelated tests on the
            // same process-wide AuditLogger don't tear our list.
            public IReadOnlyList<DriverLoadAttempt> Attempts
            {
                get { lock (_gate) { return _attempts.ToArray(); } }
            }

            public void LogDriverLoadAttempt(DriverLoadAttempt attempt)
            {
                if (_driverPathFilter != null &&
                    !string.Equals(attempt.DriverPath, _driverPathFilter, StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }
                lock (_gate) { _attempts.Add(attempt); }
            }
        }

        private sealed class PredicateAllowlist : IDriverAllowlist
        {
            private readonly Func<string, string?, bool> _predicate;
            public PredicateAllowlist(Func<string, string?, bool> predicate) => _predicate = predicate;
            public bool IsDriverAllowed(string driverPath, string? typeName) => _predicate(driverPath, typeName);
        }

        [Fact]
        public void LoadManagedDriver_AllowlistDeny_ThrowsUnauthorizedAndAudits()
        {
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);
            string typeName = typeof(FakeAdbcDriver).FullName!;

            var logger = new CapturingLogger(driverPath);
            DriverManagerSecurity.AuditLogger = logger;
            DriverManagerSecurity.Allowlist = new PredicateAllowlist((p, t) =>
                string.Equals(p, driverPath, StringComparison.OrdinalIgnoreCase) ? false : true);

            AdbcException ex = Assert.Throws<AdbcException>(
                () => AdbcDriverManager.LoadManagedDriver(driverPath, typeName));
            Assert.Equal(AdbcStatusCode.Unauthorized, ex.Status);

            DriverLoadAttempt attempt = Assert.Single(logger.Attempts);
            Assert.False(attempt.Success);
            Assert.Equal(driverPath, attempt.DriverPath);
            Assert.Equal(typeName, attempt.TypeName);
            Assert.Equal("LoadManagedDriver", attempt.LoadMethod);
            Assert.NotNull(attempt.ErrorMessage);
        }

        [Fact]
        public void LoadManagedDriver_AllowlistPermit_AuditsSuccess()
        {
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);
            string typeName = typeof(FakeAdbcDriver).FullName!;

            var logger = new CapturingLogger(driverPath);
            string? observedPath = null;
            string? observedType = null;
            DriverManagerSecurity.AuditLogger = logger;
            DriverManagerSecurity.Allowlist = new PredicateAllowlist((p, t) =>
            {
                // Only capture our own driver's invocation; tests running in parallel
                // share this process-wide allowlist.
                if (string.Equals(p, driverPath, StringComparison.OrdinalIgnoreCase))
                {
                    observedPath = p;
                    observedType = t;
                }
                return true;
            });

            AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(driverPath, typeName);
            Assert.NotNull(driver);

            // Allowlist was consulted with the resolved path and type.
            Assert.Equal(driverPath, observedPath);
            Assert.Equal(typeName, observedType);

            DriverLoadAttempt attempt = Assert.Single(logger.Attempts);
            Assert.True(attempt.Success);
            Assert.Null(attempt.ErrorMessage);
            Assert.Equal(driverPath, attempt.DriverPath);
            Assert.Equal(typeName, attempt.TypeName);
            Assert.Equal("LoadManagedDriver", attempt.LoadMethod);
        }

        [Fact]
        public void LoadManagedDriver_LoadFailure_AuditsFailure()
        {
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);
            const string missingType = "Apache.Arrow.Adbc.Tests.DriverManager.NoSuchType";

            var logger = new CapturingLogger(driverPath);
            DriverManagerSecurity.AuditLogger = logger;
            DriverManagerSecurity.Allowlist = null;

            Assert.Throws<AdbcException>(
                () => AdbcDriverManager.LoadManagedDriver(driverPath, missingType));

            DriverLoadAttempt attempt = Assert.Single(logger.Attempts);
            Assert.False(attempt.Success);
            Assert.Equal(driverPath, attempt.DriverPath);
            Assert.Equal(missingType, attempt.TypeName);
            Assert.Equal("LoadManagedDriver", attempt.LoadMethod);
            Assert.NotNull(attempt.ErrorMessage);
        }

        [Fact]
        public void LoadManagedDriver_NoAllowlistConfigured_StillAuditsSuccess()
        {
            string dir = CreateTempDir();
            string driverPath = CopyDriverAssembly(dir);
            string typeName = typeof(FakeAdbcDriver).FullName!;

            var logger = new CapturingLogger(driverPath);
            DriverManagerSecurity.AuditLogger = logger;
            DriverManagerSecurity.Allowlist = null;

            AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(driverPath, typeName);
            Assert.NotNull(driver);

            DriverLoadAttempt attempt = Assert.Single(logger.Attempts);
            Assert.True(attempt.Success);
            Assert.Equal("LoadManagedDriver", attempt.LoadMethod);
        }
    }
}
