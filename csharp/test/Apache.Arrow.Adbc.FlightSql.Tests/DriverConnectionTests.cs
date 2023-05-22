using System.Collections.Generic;
using System.IO;
using Apache.Arrow.Adbc.Core;
using Apache.Arrow.Adbc.FlightSql;
using Apache.Arrow.Adbc.FlightSql.Tests;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace Apache.Arrow.Adbc.Tests
{
    [TestClass]
    public class FlightSqlDriverConnectionTests : DriverConnectionTests
    {
        [TestMethod]
        public override void CanDriverConnect()
        {
            FlightSqlTestConfiguration flightSqlTestConfiguration = GetFlightSqlTestConfiguration();

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

            FlightSqlDriver flightSqlDriver = new FlightSqlDriver();
            FlightSqlDatabase flightSqlDatabase = flightSqlDriver.Open(parameters) as FlightSqlDatabase;
            FlightSqlConnection connection = flightSqlDatabase.Connect(options) as FlightSqlConnection;
            FlightSqlStatement statement = connection.CreateStatement() as FlightSqlStatement;

            statement.SqlQuery = flightSqlTestConfiguration.Query;
            QueryResult queryResult = statement.ExecuteQuery();

            long count = 0;

            while (true)
            {
                var nextBatch = queryResult.Stream.ReadNextRecordBatchAsync().Result;
                if (nextBatch == null) { break; }
                count += nextBatch.Length;
            }

            Assert.AreEqual(flightSqlTestConfiguration.ExpectedResultsCount, count);
        }
    
        public override void CanDriverUpdate()
        {
            throw new System.NotImplementedException();
        }

        public override void CanReadSchema()
        {
            throw new System.NotImplementedException();
        }

        public override void VerifyBadQueryGeneratesError()
        {
            throw new System.NotImplementedException();
        }

        public override void VerifyTypesAndValues()
        {
            throw new System.NotImplementedException();
        }

        private FlightSqlTestConfiguration GetFlightSqlTestConfiguration()
        {
            string json = File.ReadAllText("flightsqlconfig.pass");

            FlightSqlTestConfiguration flightSqlTestConfiguration = JsonConvert.DeserializeObject<FlightSqlTestConfiguration>(json);

            return flightSqlTestConfiguration;        
        }
    }
}
