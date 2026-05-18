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
using System.Diagnostics;
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
    /// <para>
    /// <b>Order-dependent resolution across multiple drivers:</b> if two drivers ship
    /// different versions of the same private dependency (for example
    /// <c>Newtonsoft.Json</c> 12 in one driver and 13 in another), the first driver
    /// whose resolver satisfies that simple name "wins" for the lifetime of the
    /// process; the runtime will return the already-loaded copy for every subsequent
    /// request. When a later driver's resolver could also have satisfied the same
    /// simple name, the loader emits a warning via the
    /// <c>Apache.Arrow.Adbc.DriverManager.ManagedDriverLoader</c>
    /// <see cref="TraceSource"/> so the collision is debuggable instead of silent.
    /// Callers that need version isolation between drivers should load conflicting
    /// drivers into their own <see cref="AssemblyLoadContext"/> instances.
    /// </para>
    /// <para>
    /// <b>Failed-load cleanup:</b> the per-driver <see cref="AssemblyDependencyResolver"/>
    /// is only published to the process-wide resolver cache (and the global
    /// <see cref="AssemblyLoadContext.Resolving"/> hook is only armed) after
    /// <see cref="Assembly.LoadFrom(string)"/> succeeds. If the driver assembly itself
    /// fails to load, no resolver is left behind to influence later assembly
    /// resolutions. Once a driver loads successfully its resolver remains for the
    /// lifetime of the process; the default <see cref="AssemblyLoadContext"/> is not
    /// unloadable, so there is no safe point at which to remove it.
    /// </para>
    /// </remarks>
    internal static class ManagedDriverLoader
    {
#if NET6_0_OR_GREATER
        private static readonly ConcurrentDictionary<string, AssemblyDependencyResolver> resolvers
            = new ConcurrentDictionary<string, AssemblyDependencyResolver>(StringComparer.OrdinalIgnoreCase);

        // Tracks which driver path "won" the very first contested resolution for a
        // given simple assembly name (e.g. "Newtonsoft.Json"). Used purely to emit a
        // diagnostic when a later resolver could also satisfy that name -- the runtime
        // always returns the assembly already loaded by the first winner, so any later
        // candidate is silently shadowed.
        private static readonly ConcurrentDictionary<string, string> s_firstResolverWinner
            = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        private static readonly TraceSource s_trace = new TraceSource(
            "Apache.Arrow.Adbc.DriverManager.ManagedDriverLoader", SourceLevels.Warning);

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
            string? winningPath = null;
            Assembly? loaded = null;

            foreach (var kvp in resolvers)
            {
                string? path = kvp.Value.ResolveAssemblyToPath(assemblyName);
                if (string.IsNullOrEmpty(path) || !File.Exists(path))
                {
                    continue;
                }

                if (loaded == null)
                {
                    // First resolver that can satisfy this name wins. Subsequent winners
                    // for the same simple name only matter as a diagnostic signal.
                    winningPath = path!;
                    loaded = context.LoadFromAssemblyPath(path!);

                    string simpleName = assemblyName.Name ?? path!;
                    string ownerPath = kvp.Key;
                    s_firstResolverWinner.TryAdd(simpleName, ownerPath);
                }
                else
                {
                    // Another driver also has a candidate for this name. The runtime
                    // will keep using `loaded` (the assembly already loaded by the first
                    // winner), so emit a one-shot warning so version-pinning collisions
                    // between drivers are debuggable instead of silent.
                    string simpleName = assemblyName.Name ?? path!;
                    string winnerForName = s_firstResolverWinner.GetOrAdd(simpleName, kvp.Key);
                    s_trace.TraceEvent(
                        TraceEventType.Warning,
                        id: 1,
                        format: "Managed driver dependency resolution collision for '{0}' v{1}: " +
                                "driver '{2}' would resolve to '{3}', but driver '{4}' already pinned this " +
                                "assembly for the process. Use a single shared version or load conflicting " +
                                "drivers into custom AssemblyLoadContexts.",
                        simpleName,
                        assemblyName.Version,
                        kvp.Key,
                        path,
                        winnerForName);
                }
            }

            return loaded;
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
            // Build (but do not yet register) an AssemblyDependencyResolver for this
            // driver. AssemblyDependencyResolver throws InvalidOperationException if no
            // .deps.json is present next to the assembly. Without that file the runtime
            // has no way to map the driver's transitive references to files on disk, so
            // any non-trivial driver will fail at first use. We swallow the exception so
            // the driver assembly itself can still be loaded (useful for diagnostics and
            // for fully self-contained drivers), but callers are expected to ship the
            // .deps.json.
            //
            // We intentionally do NOT add the resolver to the process-wide cache until
            // Assembly.LoadFrom below has succeeded. The default-ALC Resolving hook is a
            // permanent global side effect; registering a resolver for a driver that
            // never actually loaded would leak its dependency probing into every future
            // assembly resolution in the process.
            AssemblyDependencyResolver? pendingResolver = null;
            bool alreadyRegistered = resolvers.ContainsKey(fullPath);
            if (!alreadyRegistered)
            {
                try
                {
                    pendingResolver = new AssemblyDependencyResolver(fullPath);
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
            Assembly assembly = Assembly.LoadFrom(fullPath);

#if NET6_0_OR_GREATER
            // Driver assembly loaded successfully; now it is safe to publish the
            // resolver and arm the global Resolving hook. If LoadFrom threw, we leave
            // no trace in the process-wide cache.
            if (pendingResolver != null && resolvers.TryAdd(fullPath, pendingResolver))
            {
                EnsureResolveHookInstalled();
            }
#endif

            return assembly;
        }
    }
}
