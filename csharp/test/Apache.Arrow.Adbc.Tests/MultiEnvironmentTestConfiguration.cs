using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Tests
{
    public abstract class MultiEnvironmentTestConfiguration<T>
    {
        /// <summary>
        /// A comma separated list of testable environments.
        /// </summary>
        [JsonPropertyName("testEnvironments")]
        public List<string> TestEnvironmentNames { get; set; } = new List<string>();

        /// <summary>
        /// The active test environment.
        /// </summary>
        [JsonPropertyName("environments")]
        public Dictionary<string, T> Environments { get; set; } = new Dictionary<string, T>();
    }
}
