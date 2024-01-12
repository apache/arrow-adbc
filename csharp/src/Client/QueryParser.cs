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

namespace Apache.Arrow.Adbc.Client
{
    /// <summary>
    /// Provides a way for the caller to specify how queries are parsed.
    /// </summary>
    public class QueryConfiguration
    {
        public QueryConfiguration()
        {
            CreateKeyword = new KeywordDefinition("CREATE", QueryReturnType.RecordsAffected);
            SelectKeyword = new KeywordDefinition("SELECT", QueryReturnType.RecordSet);
            UpdateKeyword = new KeywordDefinition("UPDATE", QueryReturnType.RecordsAffected);
            DeleteKeyword = new KeywordDefinition("DELETE", QueryReturnType.RecordsAffected);
            DropKeyword   = new KeywordDefinition("DROP", QueryReturnType.RecordsAffected);
            InsertKeyword = new KeywordDefinition("INSERT", QueryReturnType.RecordsAffected);

            Keywords = new Dictionary<string,QueryReturnType>(StringComparer.OrdinalIgnoreCase)
            {
                { CreateKeyword.Keyword, CreateKeyword.ReturnType },
                { SelectKeyword.Keyword, SelectKeyword.ReturnType },
                { UpdateKeyword.Keyword, UpdateKeyword.ReturnType },
                { DeleteKeyword.Keyword, DeleteKeyword.ReturnType },
                { DropKeyword.Keyword,   DropKeyword.ReturnType },
                { InsertKeyword.Keyword, InsertKeyword.ReturnType },
            };
        }

        /// <summary>
        /// The CREATE keyword. CREATE by default.
        /// </summary>
        public KeywordDefinition CreateKeyword { get; set; }

        /// <summary>
        /// The SELECT keyword. SELECT by default.
        /// </summary>
        public KeywordDefinition SelectKeyword { get; set; }

        /// <summary>
        /// The UPDATE keyword. UPDATE by default.
        /// </summary>
        public KeywordDefinition UpdateKeyword { get; set; }

        /// <summary>
        /// The INSERT keyword. INSERT by default.
        /// </summary>
        public KeywordDefinition InsertKeyword { get; set; }

        /// <summary>
        /// The DELETE keyword. DELETE by default.
        /// </summary>
        public KeywordDefinition DeleteKeyword { get; set; }

        /// <summary>
        /// The DROP keyword. DROP by default.
        /// </summary>
        public KeywordDefinition DropKeyword { get; set; }

        /// <summary>
        /// Keywords to parse from the query. Contains CREATE, SELECT, UPDATE, INSERT, DELETE, DROP by default.
        /// </summary>
        public Dictionary<string, QueryReturnType> Keywords { get; set; }

        /// <summary>
        /// Optional. The caller can specify their own parsing function
        /// to parse the queries instead of using the default one.
        /// </summary>
        public QueryFunctionDefinition CustomParser { get; set; }
    }

    /// <summary>
    /// A keyword definition.
    /// </summary>
    public class KeywordDefinition
    {
        /// <summary>
        /// Overloaded. Initializes a <see cref="KeywordDefinition"/>.
        /// </summary>
        public KeywordDefinition()
        {

        }

        /// <summary>
        /// Overloaded. Initializes a <see cref="KeywordDefinition"/>.
        /// </summary>
        /// <param name="keyword">The keyword.</param>
        /// <param name="queryReturnType">The expected return type.</param>
        public KeywordDefinition(string keyword, QueryReturnType queryReturnType)
        {
            this.Keyword = keyword;
            this.ReturnType = queryReturnType;
        }

        /// <summary>
        /// The keyword.
        /// </summary>
        public string Keyword { get; set; }

        /// <summary>
        /// The expected return type.
        /// </summary>
        public QueryReturnType ReturnType { get; set; }
    }

    /// <summary>
    /// Defines the function definition for a custom parser.
    /// </summary>
    public class QueryFunctionDefinition
    {
        /// <summary>
        /// The second parameter for the <see cref="Parse"/> function.
        /// </summary>
        public string[] Parameter2 { get; set; }

        /// <summary>
        /// The custom function to call.
        /// </summary>
        /// <remarks>
        /// Input 1 is always the query.
        ///
        /// Input 2 is customizable using <see cref="Parameter2"/>.
        ///
        /// The return type is a string[]
        /// </remarks>
        public Func<string, string[], string[]> Parse { get; set; }
    }

    /// <summary>
    /// Parses a command text into multiple queries.
    /// </summary>
    internal class QueryParser
    {
        private QueryConfiguration _queryConfiguration = new QueryConfiguration();

        public QueryParser(QueryConfiguration queryConfiguration)
        {
            _queryConfiguration = queryConfiguration;
        }

        internal List<Query> ParseQuery(string commandText)
        {
            string[] userQueries = null;

            if(_queryConfiguration.CustomParser != null)
            {
                userQueries = _queryConfiguration.CustomParser.Parse(commandText, _queryConfiguration.CustomParser.Parameter2);
            }
            else
            {
                userQueries = SplitStringWithKeywords(commandText);
            }

            return ParseQueries(userQueries);
        }

        private List<Query> ParseQueries(string[] userQueries)
        {
            List<Query> queries = new List<Query>();

            foreach (string userQuery in userQueries)
            {
                if (string.IsNullOrEmpty(userQuery))
                    continue;

                Query query = new Query();
                query.Text = userQuery.Trim();

                string firstWord = query.Text.Split(' ')[0];

                if(this._queryConfiguration.Keywords.TryGetValue(firstWord, out QueryReturnType returnType))
                {
                    query.Type = returnType;
                    queries.Add(query);
                }
                else
                {
                    throw new InvalidOperationException($"{firstWord} is not defined as a keyword");
                }
            }

            return queries;
        }

        private string[] SplitStringWithKeywords(string input)
        {
            string[] keywords = _queryConfiguration.Keywords.Keys.ToArray();

            // Construct the regex pattern with capturing groups for keywords
            string pattern = $"({string.Join("|", keywords.Select(Regex.Escape))})";

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
    }

    /// <summary>
    /// Specifies the type of query this is
    /// </summary>
    public enum QueryReturnType
    {
        RecordSet,
        RecordsAffected
    }

    /// <summary>
    /// Represents a query to the backend specifying the text and type
    /// </summary>
    internal class Query
    {
        /// <summary>
        /// The query text.
        /// </summary>
        public string Text { get; set; }

        /// <summary>
        /// The query type
        /// </summary>
        public QueryReturnType Type { get; set; }
    }
}
