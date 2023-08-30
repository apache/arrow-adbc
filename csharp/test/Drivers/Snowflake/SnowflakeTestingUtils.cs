using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.C;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Arrow.Adbc.Tests.Drivers.Snowflake
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
    }
}
