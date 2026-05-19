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

using Xunit;

namespace Apache.Arrow.Adbc.Tests.DriverManager
{
    /// <summary>
    /// Single source of truth for the xUnit collection name shared by every
    /// test class that touches the process-wide static values on
    /// <see cref="Apache.Arrow.Adbc.DriverManager.DriverManagerSecurity"/>
    /// (notably <c>AuditLogger</c> and <c>Allowlist</c>) or that calls
    /// <see cref="Apache.Arrow.Adbc.DriverManager.AdbcDriverManager.LoadManagedDriver(string, string)"/>
    /// / <c>LoadDriver</c>, whose hot path reads those same static values.
    /// </summary>
    /// <remarks>
    /// <para>
    /// xUnit parallelizes across distinct test classes by default. Without a
    /// shared collection, a test in one class that mutates
    /// <c>DriverManagerSecurity.AuditLogger</c> or <c>Allowlist</c> can race
    /// with a load-path test in another class, producing flaky
    /// <c>Assert.Single(logger.Attempts)</c> failures or spurious
    /// "not permitted by the configured allowlist" exceptions.
    /// </para>
    /// <para>
    /// Apply <c>[Collection(DriverManagerSecurityCollection.Name)]</c> to any
    /// new test class that either mutates those static values or invokes
    /// <c>AdbcDriverManager.LoadManagedDriver</c>/<c>LoadDriver</c>.
    /// </para>
    /// </remarks>
    [CollectionDefinition(Name)]
    public sealed class DriverManagerSecurityCollection
    {
        public const string Name = "DriverManagerSecurity";
    }
}
