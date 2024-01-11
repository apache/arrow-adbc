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
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Apache.Arrow.Adbc.Client;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Client
{
    public class QueryParserTests
    {
        /// <summary>
        /// Tests the ability to parse a query.
        /// </summary>
        /// <param name="query">The query to parse.</param>
        /// <param name="firstSelectQuery">The expected select query, if present.</param>
        /// <param name="expectedQueries">The number of expected queries from parsing the query.</param>
        /// <param name="queryTypes">The expected query types.</param>
        [Theory]
        [InlineData(
            "DROP TABLE IF EXISTS TESTTABLE;CREATE TABLE TESTTABLE(intcol INT);INSERT INTO TESTTABLE VALUES (123);INSERT INTO TESTTABLE VALUES (456);SELECT * FROM TESTTABLE WHERE INTCOL=123;SELECT * FROM TESTTABLE WHERE INTCOL=456;SELECT * FROM TESTTABLE WHERE INTCOL=456;DROP TABLE TESTTABLE;",
            "SELECT * FROM TESTTABLE WHERE INTCOL=123;",
            8,
            QueryType.Drop,QueryType.Create, QueryType.Insert, QueryType.Insert,QueryType.Read, QueryType.Read,QueryType.Read, QueryType.Drop
        )]
        [InlineData(
            "DROP TABLE IF EXISTS TESTTABLE;CREATE TABLE TESTTABLE(intcol INT);INSERT INTO TESTTABLE VALUES (123);INSERT INTO TESTTABLE VALUES (456);SELECT * FROM TESTTABLE WHERE INTCOL=123;SELECT * FROM TESTTABLE WHERE INTCOL='item21;item2;item3';SELECT * FROM TESTTABLE WHERE INTCOL=456;DROP TABLE TESTTABLE;",
            "SELECT * FROM TESTTABLE WHERE INTCOL=123;",
            8,
            QueryType.Drop, QueryType.Create, QueryType.Insert, QueryType.Insert, QueryType.Read, QueryType.Read, QueryType.Read, QueryType.Drop
        )]
        [InlineData(
            "drop table if exists testtable;create table testtable(intcol int);insert into testtable values (123);insert into testtable values (456);select * from testtable where intcol=123;select * from testtable where intcol='item21;item2;item3';select * from testtable where intcol=456;drop table testtable;",
            "select * from testtable where intcol=123;",
            8,
            QueryType.Drop, QueryType.Create, QueryType.Insert, QueryType.Insert, QueryType.Read, QueryType.Read, QueryType.Read, QueryType.Drop
        )]
        [InlineData(
            "CREATE OR REPLACE TRANSIENT TABLE TESTTABLE(intcol INT);INSERT INTO TESTTABLE VALUES (123);INSERT INTO TESTTABLE VALUES (456);SELECT * FROM TESTTABLE WHERE INTCOL=123;",
            "SELECT * FROM TESTTABLE WHERE INTCOL=123;",
            4,
            QueryType.Create, QueryType.Insert, QueryType.Insert, QueryType.Read
        )]
        [InlineData(
            "select * from testtable where intcol=123",
            "select * from testtable where intcol=123",
            1,
            QueryType.Read
        )]
        [InlineData(
            "DELETE testtable where intcol=123",
            "",
            1,
            QueryType.Delete
        )]
        public void ParseQuery(string query, string firstSelectQuery, int expectedQueries, params QueryType[] queryTypes)
        {
            // uses the defaults
            QueryConfiguration qc = new QueryConfiguration();

            AssertValues(qc, query, firstSelectQuery, expectedQueries, queryTypes);

            // do the same with a custom parser
            QueryConfiguration customParsingQc = new QueryConfiguration();
            customParsingQc.CustomParser = new QueryFunctionDefinition()
            {
                Parameter2 = qc.AllKeywords,
                Parse = (input, values) =>
                {
                    // Construct the regex pattern with capturing groups for keywords
                    string pattern = $"({string.Join("|", values.Select(Regex.Escape))})";

                    string[] result = Regex.Split(input, pattern, RegexOptions.IgnoreCase);

                    // add back in the keyword that was found
                    for (int i = 1; i < result.Length; i += 2)
                    {
                        if (i < result.Length - 1)
                        {
                            result[i] += result[i + 1];
                        }
                    }

                    // Remove empty entries
                    result = result
                        .Where(s => !string.IsNullOrWhiteSpace(s))
                        .Where((_, index) => index % 2 == 0) // Only keep entries with even indices (keyword-query pairs)
                        .ToArray();

                    return result;
                }
            };

            AssertValues(customParsingQc, query, firstSelectQuery, expectedQueries, queryTypes);
        }

        private void AssertValues(QueryConfiguration qc, string query, string firstSelectQuery, int expectedQueries, params QueryType[] queryTypes)
        {
            QueryParser parser = new QueryParser(qc);

            List<Query> queries = parser.ParseQuery(query);

            string firstFoundSelectQuery = string.Empty;

            Assert.True(queries.Count == expectedQueries, $"The number of queries ({queries.Count}) does not match the expected number ({expectedQueries})");

            for (int i = 0; i < queries.Count; i++)
            {
                Query q = queries[i];

                Assert.True(q.Type == queryTypes[i], $"The value at {i} ({q.Type}) does not match the expected query type ({queryTypes[i]})");

                if (q.Type == QueryType.Read && string.IsNullOrEmpty(firstFoundSelectQuery))
                {
                    firstFoundSelectQuery = q.Text.Trim();
                }
            }

            if (!string.IsNullOrEmpty(firstSelectQuery))
            {
                Assert.True(firstSelectQuery.Equals(firstFoundSelectQuery, StringComparison.OrdinalIgnoreCase), "The expected queries do not match");
            }
        }
    }
}
