using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Apache.Arrow.Adbc.FlightSql.Tests
{
    internal class FlightSqlTestConfiguration
    {
        [JsonProperty(PropertyName="serverAddress")]
        public string ServerAddress { get; set; }

        [JsonProperty(PropertyName ="routing_tag", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string RoutingTag { get; set; }

        [JsonProperty(PropertyName = "routing_queue", DefaultValueHandling = DefaultValueHandling.Ignore)]
	    public string RoutingQueue { get; set; }

        [JsonProperty(PropertyName = "authorization")]
        public string Authorization { get; set; }

        [JsonProperty(PropertyName = "query")]
        public string Query { get; set; }

        [JsonProperty(PropertyName = "expectedResults")]
        public long ExpectedResultsCount { get; set; }
    }
}
