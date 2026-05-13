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
    ///       Drivers should be published with their <c>.deps.json</c> file alongside the
    ///       assembly (the default when building with the .NET SDK). Native dependencies
    ///       should follow the standard <c>runtimes/&lt;rid&gt;/native</c> layout.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       If a driver does not ship a <c>.deps.json</c>, the loader still works but
    ///       falls back to whatever the host can resolve plus the driver's directory.
    ///       Callers needing strong isolation should load the driver into a custom
    ///       <see cref="AssemblyLoadContext"/> themselves.
    ///     </description>
    ///   </item>
    /// </list>
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
            // resolve a dependency. AssemblyDependencyResolver throws if no .deps.json
            // is present next to the assembly; in that case we silently fall back to
            // the host's normal probing behavior.
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
                    // No .deps.json next to the driver; rely on LoadFrom's directory probing.
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
