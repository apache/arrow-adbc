using System;
using System.Collections.Generic;
using System.IO;
using Apache.Arrow.Adbc.Client;
using Apache.Arrow.Adbc.Drivers.FlightSql;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Arrow.Adbc.Tests.Drivers.FlightSql
{
    [TestClass]
    public class ClientTests
    {
        [TestMethod]
        public void CanFlightSqlConnectUsingClient()
        {
            FlightSqlTestConfiguration flightSqlTestConfiguration = Utils.GetTestConfiguration<FlightSqlTestConfiguration>("flightsqlconfig.json");

            Dictionary<string, string> parameters = new Dictionary<string, string>
            {
                { FlightSqlParameters.ServerAddress, flightSqlTestConfiguration.ServerAddress },
                { FlightSqlParameters.RoutingTag, flightSqlTestConfiguration.RoutingTag },
                { FlightSqlParameters.RoutingQueue, flightSqlTestConfiguration.RoutingQueue },
                { FlightSqlParameters.Authorization, flightSqlTestConfiguration.Authorization}
            };

            Dictionary<string, string> options = new Dictionary<string, string>()
            {
                { FlightSqlParameters.ServerAddress, flightSqlTestConfiguration.ServerAddress },
            };

            long count = 0;

            using (Client.AdbcConnection adbcConnection = new Client.AdbcConnection(
                new FlightSqlDriver(),
                parameters,
                options)
            )
            {
                string query = flightSqlTestConfiguration.Query;

                AdbcCommand adbcCommand = new AdbcCommand(query, adbcConnection);

                adbcConnection.Open();

                AdbcDataReader reader = adbcCommand.ExecuteReader();

                try
                {
                    while (reader.Read())
                    {
                        count++;
                    }
                }
                finally { reader.Close(); }
            }

            Assert.AreEqual(flightSqlTestConfiguration.ExpectedResultsCount, count);
        }
    }
}
