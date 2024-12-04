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

namespace Apache.Arrow.Adbc.Drivers.Interop.Snowflake
{
    /// <summary>
    /// Lightweight class for loading the Snowflake Go driver to .NET.
    /// </summary>
    public class SnowflakeDriverLoader : AdbcDriverLoader
    {
        public SnowflakeDriverLoader() : base("libadbc_driver_snowflake", "SnowflakeDriverInit")
        {

        }

        /// <summary>
        /// Loads the Snowflake Go driver from the current directory using the default name and entry point.
        /// </summary>
        /// <returns>An <see cref="AdbcDriver"/> based on the Snowflake Go driver.</returns>
        /// <exception cref="FileNotFoundException"></exception>
        public static AdbcDriver LoadDriver()
        {
            return new SnowflakeDriverLoader().FindAndLoadDriver();
        }
    }
}
