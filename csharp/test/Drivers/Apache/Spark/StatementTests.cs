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
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    public class StatementTests : Common.StatementTests<SparkTestConfiguration, SparkTestEnvironment>
    {
        public StatementTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new SparkTestEnvironment.Factory())
        {
        }
    }

    /// <summary>
    /// Data type used for metadata timeout tests.
    /// </summary>
    internal class StatementWithExceptions
    {
        public StatementWithExceptions(int? queryTimeoutSeconds, string? query, Type? exceptionType)
        {
            QueryTimeoutSeconds = queryTimeoutSeconds;
            Query = query;
            ExceptionType = exceptionType;
        }

        /// <summary>
        /// If null, uses the default timeout.
        /// </summary>
        public int? QueryTimeoutSeconds { get; }

        /// <summary>
        /// If null, expected to succeed.
        /// </summary>
        public Type? ExceptionType { get; }

        /// <summary>
        /// If null, uses the default TestConfiguration.
        /// </summary>
        public string? Query { get; }
    }

    /// <summary>
    /// Collection of <see cref="StatementWithExceptions"/> for testing statement timeouts."/>
    /// </summary>
    internal class StatementTimeoutTestData : TheoryData<StatementWithExceptions>
    {
        public StatementTimeoutTestData()
        {
            string longRunningQuery = "SELECT COUNT(*) AS total_count\nFROM (\n  SELECT t1.id AS id1, t2.id AS id2\n  FROM RANGE(1000000) t1\n  CROSS JOIN RANGE(10000) t2\n) subquery\nWHERE MOD(id1 + id2, 2) = 0";

            Add(new(0, null, null));
            Add(new(null, null, null));
            Add(new(1, null, typeof(TimeoutException)));
            Add(new(5, null, null));
            Add(new(30, null, null));
            Add(new(5, longRunningQuery, typeof(TimeoutException)));
            Add(new(null, longRunningQuery, typeof(TimeoutException)));
            Add(new(0, longRunningQuery, null));
        }
    }
}
