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
using System.IO;
using System.Runtime.InteropServices;
using Apache.Arrow.Adbc.C;

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// Lightweight class for loading an Interop driver to .NET.
    /// </summary>
    public abstract class AdbcDriverLoader
    {
        readonly string driverShortName;
        readonly string entryPoint;

        /// <summary>
        /// Initializes the driver loader with the driver name and entry point.
        /// </summary>
        /// <param name="driverShortName">Short driver name, with no extension.</param>
        /// <param name="entryPoint">The entry point. Defaults to `AdbcDriverInit` if not provided.</param>
        /// <exception cref="ArgumentException"></exception>
        protected AdbcDriverLoader(string driverShortName, string entryPoint = "AdbcDriverInit")
        {
            if (string.IsNullOrEmpty(driverShortName))
                throw new ArgumentException("cannot be null or empty", nameof(driverShortName));

            if (string.IsNullOrEmpty(entryPoint))
                throw new ArgumentException("cannot be null or empty", nameof(entryPoint));

            this.driverShortName = driverShortName;
            this.entryPoint = entryPoint;
        }

        /// <summary>
        /// Loads the Interop from the current directory using the default name and entry point.
        /// </summary>
        /// <returns>An <see cref="AdbcDriver"/> based on the Flight SQL Go driver.</returns>
        /// <exception cref="FileNotFoundException"></exception>
        protected AdbcDriver FindAndLoadDriver()
        {
            string root = "runtimes";
            string native = "native";
            string architecture = RuntimeInformation.OSArchitecture.ToString().ToLower();
            string fileName = driverShortName;
            string file;

            // matches extensions in https://github.com/apache/arrow-adbc/blob/main/go/adbc/pkg/Makefile
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                file = Path.Combine(root, $"linux-{architecture}", native, $"{fileName}.so");
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                file = Path.Combine(root, $"win-{architecture}", native, $"{fileName}.dll");
            else
                file = Path.Combine(root, $"osx-{architecture}", native, $"{fileName}.dylib");

            if (File.Exists(file))
            {
                // get the full path because some .NET versions need it
                file = Path.GetFullPath(file);
            }
            else
            {
                throw new FileNotFoundException($"Could not find {file}");
            }

            return LoadDriver(file, entryPoint);
        }

        /// <summary>
        /// Loads the Interop driver from the current directory using the default name and entry point.
        /// </summary>
        /// <param name="file">The file to load.</param>
        /// <param name="entryPoint">The entry point of the file.</param>
        /// <returns>An <see cref="AdbcDriver"/>.</returns>
        public static AdbcDriver LoadDriver(string file, string entryPoint)
        {
            AdbcDriver driver = CAdbcDriverImporter.Load(file, entryPoint);

            return driver;
        }
    }
}
