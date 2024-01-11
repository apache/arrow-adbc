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
using System.Text;
using System.Text.RegularExpressions;

namespace Apache.Arrow.Adbc.Client
{
    /// <summary>
    /// Provides a way for the caller to specify how queries are parsed.
    /// </summary>
    public class QueryConfiguration
    {
        private string[] _keywords = new string[] { };

        public QueryConfiguration()
        {
            CreateKeyword = "CREATE";
            SelectKeyword = "SELECT";
            UpdateKeyword = "UPDATE";
            DeleteKeyword = "DELETE";
            DropKeyword = "DROP";
            InsertKeyword = "INSERT";

            this.AdditionalKeywords = new string[] { };
        }

        /// <summary>
        /// The CREATE keyword. CREATE by default.
        /// </summary>
        public string CreateKeyword { get; set; }

        /// <summary>
        /// The SELECT keyword. SELECT by default.
        /// </summary>
        public string SelectKeyword { get; set; }

        /// <summary>
        /// The UPDATE keyword. UPDATE by default.
        /// </summary>
        public string UpdateKeyword { get; set; }

        /// <summary>
        /// The INSERT keyword. INSERT by default.
        /// </summary>
        public string InsertKeyword { get; set; }

        /// <summary>
        /// The DELETE keyword. DELETE by default.
        /// </summary>
        public string DeleteKeyword { get; set; }

        /// <summary>
        /// The DROP keyword. DROP by default.
        /// </summary>
        public string DropKeyword { get; set; }

        /// <summary>
        /// Optional additional keywords.
        /// </summary>
        public string[] AdditionalKeywords { get; set; }

        /// <summary>
        /// All of the keywords that have been passed.
        /// </summary>
        public string[] AllKeywords
        {
            get
            {
                if(_keywords.Length > 0)
                {
                    return _keywords;
                }

                List<string> keywords = new List<string>()
                {
                    CreateKeyword,
                    SelectKeyword,
                    UpdateKeyword,
                    InsertKeyword,
                    DeleteKeyword,
                    DropKeyword
                };

                foreach(string kw in AdditionalKeywords)
                {
                    keywords.Add(kw);
                }

                _keywords = keywords.ToArray();

                return _keywords;
            }
        }

        /// <summary>
        /// Optional. The caller can specify their own parsing function
        /// to parse the queries instead of using the default one.
        /// </summary>
        public QueryFunctionDefinition CustomParser { get; set; }
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
                userQueries = SplitStringWithKeywords(commandText, _queryConfiguration.AllKeywords);
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

                if (query.Text.ToUpper().StartsWith(_queryConfiguration.CreateKeyword))
                    query.Type = QueryType.Create;
                else if (query.Text.ToUpper().StartsWith(_queryConfiguration.SelectKeyword))
                    query.Type = QueryType.Read;
                else if (query.Text.ToUpper().StartsWith(_queryConfiguration.InsertKeyword))
                    query.Type = QueryType.Insert;
                else if (query.Text.ToUpper().StartsWith(_queryConfiguration.UpdateKeyword))
                    query.Type = QueryType.Update;
                else if (query.Text.ToUpper().StartsWith(_queryConfiguration.DeleteKeyword))
                    query.Type = QueryType.Delete;
                else if (query.Text.ToUpper().StartsWith(_queryConfiguration.DropKeyword))
                    query.Type = QueryType.Drop;
                else
                    throw new InvalidOperationException("unable to parse query");

                queries.Add(query);
            }

            return queries;
        }

        private string[] SplitStringWithKeywords(string input, string[] keywords)
        {
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
    public enum QueryType
    {
        Create,
        Read,
        Insert,
        Update,
        Delete,
        Drop
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
        public QueryType Type { get; set; }
    }
}
