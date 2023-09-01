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
using Apache.Arrow.Adbc.C;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    internal class SnowflakeTestingUtils
    {
        /// <summary>
        /// Gets a the Snowflake ADBC driver with settings from the <see cref="SnowflakeTestConfiguration"/>.
        /// </summary>
        /// <param name="testConfiguration"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        internal static AdbcDriver GetSnowflakeAdbcDriver(
            SnowflakeTestConfiguration testConfiguration,
            out Dictionary<string, string> parameters
           )
        {
            // see https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html

            parameters = new Dictionary<string, string>
            {
                { "adbc.snowflake.sql.account", testConfiguration.Account },
                { "username", testConfiguration.User },
                { "password", testConfiguration.Password },
                { "adbc.snowflake.sql.warehouse", testConfiguration.Warehouse },
                { "adbc.snowflake.sql.auth_type", testConfiguration.AuthenticationType }
            };

            Dictionary<string, string> options = new Dictionary<string, string>() { };
            AdbcDriver snowflakeDriver = CAdbcDriverImporter.Load(testConfiguration.DriverPath, testConfiguration.DriverEntryPoint);

            return snowflakeDriver;
        }

        /// <summary>
        /// Gets a the Snowflake ADBC driver with settings from the <see cref="SnowflakeTestConfiguration"/>.
        /// </summary>
        /// <param name="testConfiguration"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        internal static AdbcDriver GetSnowflakeAdbcDriver(
            SnowflakeTestConfiguration testConfiguration
           )
        {
            Dictionary<string, string> options = new Dictionary<string, string>() { };
            AdbcDriver snowflakeDriver = CAdbcDriverImporter.Load(testConfiguration.DriverPath, testConfiguration.DriverEntryPoint);

            return snowflakeDriver;
        }
    }
}
