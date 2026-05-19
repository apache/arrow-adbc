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

using System.Runtime.InteropServices;
using Apache.Arrow.Adbc.C;

namespace Apache.Arrow.Adbc.TestFixture.Native
{
    /// <summary>
    /// Native entry point exposed by this AOT-published shared library.
    ///
    /// <para>The ADBC driver manager looks up a symbol named <c>AdbcDriverInit</c>
    /// (the default; callers may override via <c>entrypoint=</c>). When called,
    /// this method delegates to <see cref="CAdbcDriverExporter.AdbcDriverInit"/>
    /// wrapped around a fresh <see cref="FixtureDriver"/>.</para>
    ///
    /// <para>One <see cref="FixtureDriver"/> instance is created per load; the
    /// exporter takes ownership via a GCHandle stored on the CAdbcDriver and
    /// disposes it when the driver is released.</para>
    /// </summary>
    public static unsafe class DriverInit
    {
        [UnmanagedCallersOnly(EntryPoint = "AdbcDriverInit")]
        public static AdbcStatusCode AdbcDriverInit(int version, CAdbcDriver* driver, CAdbcError* error)
        {
            return CAdbcDriverExporter.AdbcDriverInit(version, driver, error, new FixtureDriver());
        }
    }
}
