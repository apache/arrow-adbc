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

using System.IO;
using Apache.Arrow.Adbc.C;

namespace Apache.Arrow.Adbc.Drivers.Interop.Snowflake
{
    /// <summary>
    /// Lightweight class for loading the Snowflake Go driver to .NET.
    /// </summary>
    public class SnowflakeDriverLoader
    {
        /// <summary>
        /// Loads the Snowflake Go driver from the current directory using the default name and entry point.
        /// </summary>
        /// <returns>An <see cref="AdbcDriver"/> based on the Snowflake Go driver.</returns>
        /// <exception cref="FileNotFoundException"></exception>
        public static AdbcDriver LoadDriver()
        {
            string file = "libadbc_driver_snowflake.dll";

            if(File.Exists(file))
            {
                // get the full path because some .NET versions need it
                file = Path.GetFullPath(file);
            }
            else
            {
                throw new FileNotFoundException($"Cound not find {file}");
            }

            return LoadDriver(file, "SnowflakeDriverInit");
        }

        /// <summary>
        /// Loads the Snowflake Go driver from the current directory using the default name and entry point.
        /// </summary>
        /// <param name="file">The file to load.</param>
        /// <param name="entryPoint">The entry point of the file.</param>
        /// <returns>An <see cref="AdbcDriver"/> based on the Snowflake Go driver.</returns>
        public static AdbcDriver LoadDriver(string file, string entryPoint)
        {
            AdbcDriver snowflakeDriver = CAdbcDriverImporter.Load(file, entryPoint);

            return snowflakeDriver;
        }
    }
}
