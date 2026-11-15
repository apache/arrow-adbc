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

using System.Collections.Generic;

namespace Apache.Arrow.Adbc.Tests.DriverManager
{
    /// <summary>
    /// Minimal in-memory AdbcDriver used to exercise the managed-driver loading path
    /// without requiring a real driver assembly on disk.
    /// </summary>
    public sealed class FakeAdbcDriver : AdbcDriver
    {
        /// <summary>The parameters passed to the most recent <see cref="Open"/> call.</summary>
        public IReadOnlyDictionary<string, string>? LastOpenParameters { get; private set; }

        public override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters)
        {
            LastOpenParameters = parameters;
            return new FakeAdbcDatabase(parameters);
        }
    }

    /// <summary>Minimal in-memory AdbcDatabase returned by <see cref="FakeAdbcDriver"/>.</summary>
    public sealed class FakeAdbcDatabase : AdbcDatabase
    {
        public IReadOnlyDictionary<string, string> Parameters { get; }

        public FakeAdbcDatabase(IReadOnlyDictionary<string, string> parameters)
        {
            Parameters = parameters;
        }

        public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options) =>
            throw new System.NotImplementedException();
    }

    /// <summary>
    /// Minimal in-memory AdbcDriver that accepts typed options, used to verify that
    /// the driver manager preserves option types.
    /// </summary>
    public sealed class FakeTypedAdbcDriver : AdbcDriver
    {
        public override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters) =>
            throw new System.InvalidOperationException("Expected the typed Open overload to be called.");

        public override AdbcDatabase Open(IReadOnlyDictionary<string, object> parameters) =>
            new FakeTypedAdbcDatabase(parameters);
    }

    /// <summary>Minimal in-memory AdbcDatabase returned by <see cref="FakeTypedAdbcDriver"/>.</summary>
    public sealed class FakeTypedAdbcDatabase : AdbcDatabase
    {
        public IReadOnlyDictionary<string, object> Parameters { get; }

        public FakeTypedAdbcDatabase(IReadOnlyDictionary<string, object> parameters)
        {
            Parameters = parameters;
        }

        public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options) =>
            throw new System.NotImplementedException();
    }
}
