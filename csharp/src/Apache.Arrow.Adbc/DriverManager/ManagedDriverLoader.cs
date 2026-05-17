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
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;
using System.Threading;
#if NET6_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
using System.Runtime.Loader;
#endif

namespace Apache.Arrow.Adbc.DriverManager
{
    /// <summary>
    /// Provides cross-platform assembly loading functionality for managed ADBC drivers.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This type abstracts the differences in assembly loading between .NET Framework
    /// (which uses Fusion / <c>LoadFrom</c> probing) and modern .NET (which uses
    /// <c>AssemblyLoadContext</c> and <c>.deps.json</c>-driven resolution).
    /// </para>
    /// <para>
    /// <b>Dependency resolution on .NET Framework (net472, netstandard2.0 build):</b>
    /// <see cref="Assembly.LoadFrom(string)"/> probes the assembly's directory for
    /// dependencies. Drop the driver assembly and all of its dependencies in the same
    /// directory and they will be found.
    /// </para>
    /// <para>
    /// <b>Dependency resolution on .NET 6 / .NET 8 / .NET 10:</b>
    /// </para>
    /// <list type="bullet">
    ///   <item>
    ///     <description>
    ///       The driver assembly is loaded with <see cref="Assembly.LoadFrom(string)"/>
    ///       into <see cref="AssemblyLoadContext.Default"/>. This is intentional: it keeps
    ///       a single identity for <c>Apache.Arrow.Adbc.AdbcDriver</c> across the host
    ///       and the driver so that <c>IsAssignableFrom</c> checks work correctly.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       Loading into the default context means the <b>host's</b> <c>.deps.json</c>
    ///       drives resolution, not the driver's. Dependencies that are not already known
    ///       to the host will not be found by default. To bridge that gap, this loader
    ///       registers an <see cref="AssemblyDependencyResolver"/> for each driver
    ///       assembly and attaches a single
    ///       <see cref="AssemblyLoadContext.Resolving"/> handler to
    ///       <see cref="AssemblyLoadContext.Default"/>. When the runtime fails to resolve
    ///       a dependency, the handler consults each registered driver's resolver, which
    ///       reads the driver's <c>.deps.json</c> next to it on disk.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       Drivers <b>must</b> be published with their <c>.deps.json</c> file alongside
    ///       the assembly (the default when building with the .NET SDK). Without it the
    ///       loader cannot construct an <see cref="AssemblyDependencyResolver"/>, the
    ///       host's TPA list is the only thing that gets consulted, and any driver
    ///       dependency the host does not already have will fail to load at the first
    ///       call site that touches it. Native dependencies should follow the standard
    ///       <c>runtimes/&lt;rid&gt;/native</c> layout.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       A driver whose only dependencies are already present in the host's TPA list
    ///       can technically load without a <c>.deps.json</c>, but this is fragile and
    ///       not a supported configuration. Callers needing strong isolation should
    ///       load the driver into a custom <see cref="AssemblyLoadContext"/> themselves.
    ///     </description>
    ///   </item>
    /// </list>
    /// <para>
    /// <b>Shared public contract assemblies and version skew:</b> a small set of
    /// assemblies form the public contract between the driver manager and any managed
    /// driver and must have <i>exactly one identity</i> per process. In particular:
    /// </para>
    /// <list type="bullet">
    ///   <item><description><c>Apache.Arrow.Adbc</c> (defines <see cref="AdbcDriver"/>, <c>AdbcDatabase</c>, etc.)</description></item>
    ///   <item><description><c>Apache.Arrow</c> (Arrow types crossing the API boundary)</description></item>
    ///   <item><description><c>System.Diagnostics.DiagnosticSource</c> (telemetry / Activity)</description></item>
    /// </list>
    /// <para>
    /// Because the driver is loaded into the default <see cref="AssemblyLoadContext"/>,
    /// the runtime always returns the copy already loaded by the host for these
    /// assemblies; any version shipped next to the driver and listed in the driver's
    /// <c>.deps.json</c> is ignored. This is the desired behavior (type identity is
    /// preserved across the boundary) but it has an operational consequence:
    /// <b>the host must ship versions of these contract assemblies that meet or exceed
    /// the minimum versions the driver was compiled against.</b> If the host pins an
    /// older version, the driver may throw <see cref="MissingMethodException"/> or
    /// <see cref="TypeLoadException"/> the first time it touches a newer API.
    /// </para>
    /// <para>
    /// Private driver dependencies (i.e. assemblies the host does <i>not</i> reference)
    /// go through the per-driver <see cref="AssemblyDependencyResolver"/> and load from
    /// the driver's directory using its <c>.deps.json</c>. They are isolated from host
    /// versions of the same files only insofar as the host has not already loaded them.
    /// </para>
    /// </remarks>
    internal static class ManagedDriverLoader
    {
#if NET6_0_OR_GREATER
        private static readonly ConcurrentDictionary<string, AssemblyDependencyResolver> resolvers
            = new ConcurrentDictionary<string, AssemblyDependencyResolver>(StringComparer.OrdinalIgnoreCase);

        private static int s_resolveHookInstalled;

        private static void EnsureResolveHookInstalled()
        {
            if (Interlocked.CompareExchange(ref s_resolveHookInstalled, 1, 0) == 0)
            {
                AssemblyLoadContext.Default.Resolving += OnDefaultContextResolving;
            }
        }

        [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
            Justification = "Dynamic driver loading is inherently incompatible with full trimming. " +
                            "Drivers must opt out of trimming or be preserved by the host.")]
        private static Assembly? OnDefaultContextResolving(AssemblyLoadContext context, AssemblyName assemblyName)
        {
            foreach (AssemblyDependencyResolver resolver in resolvers.Values)
            {
                string? path = resolver.ResolveAssemblyToPath(assemblyName);
                if (!string.IsNullOrEmpty(path) && File.Exists(path))
                {
                    return context.LoadFromAssemblyPath(path!);
                }
            }
            return null;
        }
#endif

        /// <summary>
        /// Loads a managed driver assembly from the specified path.
        /// </summary>
        /// <param name="assemblyPath">The path to the assembly to load.</param>
        /// <returns>The loaded assembly.</returns>
        /// <exception cref="FileNotFoundException">The assembly file was not found.</exception>
        /// <exception cref="BadImageFormatException">The file is not a valid managed assembly.</exception>
        /// <remarks>
        /// See the documentation on <see cref="ManagedDriverLoader"/> for dependency
        /// resolution semantics on .NET Framework versus modern .NET.
        /// </remarks>
#if NET6_0_OR_GREATER
        [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
            Justification = "Dynamic driver loading is inherently incompatible with full trimming. " +
                            "Drivers must opt out of trimming or be preserved by the host.")]
#endif
        internal static Assembly LoadAssembly(string assemblyPath)
        {
            string fullPath = Path.GetFullPath(assemblyPath);

            if (!File.Exists(fullPath))
            {
                throw new FileNotFoundException($"Assembly not found: {fullPath}", fullPath);
            }

#if NET6_0_OR_GREATER
            // Register an AssemblyDependencyResolver for this driver so that its own
            // .deps.json is consulted when the default AssemblyLoadContext fails to
            // resolve a dependency. AssemblyDependencyResolver throws InvalidOperationException
            // if no .deps.json is present next to the assembly. Without that file the
            // runtime has no way to map the driver's transitive references to files on
            // disk, so any non-trivial driver will fail at first use. We swallow the
            // exception so the driver assembly itself can still be loaded (useful for
            // diagnostics and for fully self-contained drivers), but callers are
            // expected to ship the .deps.json.
            if (!resolvers.ContainsKey(fullPath))
            {
                try
                {
                    AssemblyDependencyResolver resolver = new AssemblyDependencyResolver(fullPath);
                    resolvers.TryAdd(fullPath, resolver);
                    EnsureResolveHookInstalled();
                }
                catch (InvalidOperationException)
                {
                    // No .deps.json next to the driver. The driver assembly will still
                    // load, but any transitive dependency not already in the host's TPA
                    // list will fail to resolve.
                }
            }
#endif

            // Use LoadFrom which works on both .NET Framework and modern .NET.
            // On modern .NET this loads into AssemblyLoadContext.Default, which keeps
            // a single identity for shared types such as AdbcDriver.
            return Assembly.LoadFrom(fullPath);
        }
    }
}
