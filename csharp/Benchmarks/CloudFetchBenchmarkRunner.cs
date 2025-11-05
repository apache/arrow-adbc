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

using Apache.Arrow.Adbc.Benchmarks.Databricks;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
#if NET472
using System.Net;
#endif

namespace Apache.Arrow.Adbc.Benchmarks
{
    /// <summary>
    /// Standalone runner for CloudFetch benchmarks only.
    /// Usage: dotnet run -c Release --framework net8.0 CloudFetchBenchmarkRunner
    /// </summary>
    public class CloudFetchBenchmarkRunner
    {
        public static void Main(string[] args)
        {
#if NET472
            // Enable TLS 1.2/1.3 for .NET Framework 4.7.2 (required for modern HTTPS endpoints)
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | (SecurityProtocolType)3072; // 3072 = Tls13
#endif
            // Configure to include the peak memory column and hide confusing error column
            var config = DefaultConfig.Instance
                .AddColumn(new PeakMemoryColumn())
                .HideColumns("Error", "StdDev");  // Hide statistical columns that are confusing with few iterations

            // Run only the real E2E CloudFetch benchmark
            var summary = BenchmarkSwitcher.FromTypes(new[] {
                typeof(CloudFetchRealE2EBenchmark)          // Real E2E with Databricks (requires credentials)
            }).Run(args, config);
        }
    }
}
