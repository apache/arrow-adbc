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

namespace Apache.Arrow.Adbc.Drivers.Databricks.Result
{
    /// <summary>
    /// Represents the result of a DESC EXTENDED TABLE query in Databricks.
    /// </summary>
    internal class DescTableExtendedResult
    {
        [JsonPropertyName("table_name")]
        public string TableName { get; set; } = default!;

        [JsonPropertyName("catalog_name")]
        public string? CatalogName { get; set; }

        [JsonPropertyName("schema_name")]
        public string SchemaName { get; set; } = default!;

        [JsonPropertyName("type")]
        public string Type { get; set; } = default!;

        [JsonPropertyName("columns")]
        public List<Column> Columns { get; set; } = new();

        [JsonPropertyName("table_properties")]
        public Dictionary<string, string> TableProperties { get; set; } = new();

        [JsonPropertyName("table_constraints")]
        public string? TableConstraints { get; set; }

        public class Column
        {
            [JsonPropertyName("name")]
            public string Name { get; set; } = default!;

            [JsonPropertyName("type")]
            public Dictionary<string, object> Type { get; set; }

            [JsonPropertyName("comment")]
            public string? Comment { get; set; }

            [JsonPropertyName("nullable")]
            public bool? Nullable { get; set; }

            [JsonPropertyName("default")]
            public string? Default { get; set; }
        }

        public class ForeignKeyInfo
        {
            public List<string> LocalColumns { get; set; } = new List<string>();
            public List<string> RefColumns { get; set; } = new List<string>();
            public string RefCatalog { get; set; } = string.Empty;
            public string RefSchema { get; set; } = string.Empty;
            public string RefTable { get; set; } = string.Empty;
        }


        private bool _hasConstraintsParsed = false;

        private List<string> _primaryKeys = new();
        private List<ForeignKeyInfo> _foreignKeys = new();

        [JsonIgnore]
        public List<string> PrimaryKeys
        {
            get
            {
                parseConstraints();
                return string.Join(", ", _primaryKeys);
            }
        }

        [JsonIgnore]
        public List<ForeignKeyInfo> ForeignKeys
        {
            get
            {
                parseConstraints();
                return _foreignKeys;
            }
        }

        private void parseConstraints()
        {
            if (_hasConstraintsParsed)
                return;

            _hasConstraintsParsed = true;
        }
    }
}