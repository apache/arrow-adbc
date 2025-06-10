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
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using static Apache.Arrow.Adbc.Drivers.Apache.Hive2.HiveServer2Connection;


namespace Apache.Arrow.Adbc.Drivers.Databricks.Result
{
    /// <summary>
    /// The response of SQL `DESC EXTENDED TABLE <table_name> AS JSON`
    /// 
    /// See https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table#json-formatted-output
    /// </summary>
    internal class DescTableExtendedResult
    {
      
        [JsonPropertyName("table_name")]
        public string TableName { get; set; } = String.Empty;

        [JsonPropertyName("catalog_name")]
        public string CatalogName { get; set; } = String.Empty;

        [JsonPropertyName("schema_name")]
        public string SchemaName { get; set; } = String.Empty;

        [JsonPropertyName("type")]
        public string Type { get; set; } = String.Empty;

        [JsonPropertyName("columns")]
        public List<ColumnInfo> Columns { get; set; } = new List<ColumnInfo>();

        [JsonPropertyName("table_properties")]
        public Dictionary<string, string> TableProperties { get; set; } = new Dictionary<string, string>();

        /// <summary>
        /// Table constraints in a string format, e.g.:
        /// 
        /// "[ (pk_constraint, PRIMARY KEY (`col1`, `col2`)),
        ///    (fk_constraint, FOREIGN KEY (`col3`) REFERENCES `catalog`.`schema`.`table` (`refcol1`, `refcol2`))
        ///  ]"
        /// </summary>
        [JsonPropertyName("table_constraints")]
        public string? TableConstraints { get; set; }

        internal class ColumnInfo
        {
            [JsonPropertyName("name")]
            public string Name { get; set; } = String.Empty;

            [JsonPropertyName("type")]
            public ColumnType Type { get; set; } = new ColumnType();

            [JsonPropertyName("comment")]
            public string? Comment { get; set; }

            [JsonPropertyName("nullable")]
            public bool Nullable { get; set; } = true;

            [JsonIgnore]
            public ColumnTypeId DataType
            {
                get
                {
                    // Supported type name: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-datatypes

                    string normalizedTypeName = Type.Name.Trim().ToUpper();

                    return normalizedTypeName switch
                    {
                        "BOOLEAN" => ColumnTypeId.BOOLEAN,
                        "TINYINT" or "BYTE" => ColumnTypeId.TINYINT,
                        "SMALLINT" or "SHORT" => ColumnTypeId.SMALLINT,
                        "INT" or "INTEGER" => ColumnTypeId.INTEGER,
                        "BIGINT" or "LONG" => ColumnTypeId.BIGINT,
                        "FLOAT" or "REAL" => ColumnTypeId.FLOAT,
                        "DOUBLE" => ColumnTypeId.DOUBLE,
                        "DECIMAL" or "NUMERIC" => ColumnTypeId.DECIMAL,

                        "CHAR" => ColumnTypeId.CHAR, 
                        "STRING" or "VARCHAR" => ColumnTypeId.VARCHAR,
                        "BINARY" => ColumnTypeId.BINARY,
                        
                        "TIMESTAMP" => ColumnTypeId.TIMESTAMP,
                        "TIMESTAMP_LTZ" => ColumnTypeId.TIMESTAMP_WITH_TIMEZONE,
                        "TIMESTAMP_NTZ" => ColumnTypeId.TIMESTAMP,
                        "DATE" => ColumnTypeId.DATE,

                        "ARRAY" => ColumnTypeId.ARRAY,
                        "MAP" => ColumnTypeId.JAVA_OBJECT,
                        "STRUCT" => ColumnTypeId.STRUCT,
                        "INTERVAL" => ColumnTypeId.OTHER, // Intervals don't have a direct JDBC mapping
                        "VOID" => ColumnTypeId.NULL,
                        "VARIANT" => ColumnTypeId.OTHER,
                        _ => ColumnTypeId.OTHER // Default fallback for unknown types
                    };
                }
            }
        }

        public class ForeignKeyInfo
        {
            public List<string> LocalColumns { get; set; } = new List<string>();
            public List<string> RefColumns { get; set; } = new List<string>();
            public string RefCatalog { get; set; } = string.Empty;
            public string RefSchema { get; set; } = string.Empty;
            public string RefTable { get; set; } = string.Empty;
        }

        internal class ColumnType
        {
            [JsonPropertyName("name")]
            public string Name { get; set; } = String.Empty;

            [JsonPropertyName("precision")]
            public int? Precision { get; set; }

            [JsonPropertyName("scale")]
            public int? Scale { get; set; }

            [JsonPropertyName("element_type")]
            public ColumnType? ElementType { get; set; }

            [JsonPropertyName("key_type")]
            public ColumnType? KeyType { get; set; }

            [JsonPropertyName("value_type")]
            public ColumnType? ValueType { get; set; }

            [JsonPropertyName("fields")]
            public List<ColumnInfo>? Fields { get; set; }

            [JsonPropertyName("start_unit")]
            public string? StartUnit { get; set; }

            [JsonPropertyName("end_unit")]
            public string? EndUnit { get; set; }
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
                return _primaryKeys;
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

            if (TableConstraints == null || TableConstraints.Trim().Length == 0)
                return;

            // Constraints string format example:
            // "[ (pk_constraint, PRIMARY KEY (`col1`, `col2`)), (fk_constraint, FOREIGN KEY (`col3`) REFERENCES `catalog`.`schema`.`table` (`refcol1`, `refcol2`)) ]"

            var constraintString = TableConstraints.Trim();
    
            if (!constraintString.StartsWith("[") || !constraintString.EndsWith("]"))
            {
                throw new FormatException($"Invalid table constraints format. {TableConstraints}");
            }

            // Remove the outer brackets
            var innerContent = constraintString.Substring(1, constraintString.Length-2).Trim(); 

            // Parse individual constraints manually to handle backtick-quoted identifiers with special characters
            var constraints = ParseConstraintList(innerContent);

            foreach (var constraint in constraints)
            {
                var constraintName = constraint.Name;
                var constraintDef = constraint.Definition;

                if (constraintDef.StartsWith("PRIMARY KEY"))
                {
                    // Parse PRIMARY KEY constraint
                    // Pattern: PRIMARY KEY (`col1`, `col2`, ...)
                    var columns = ExtractColNames(constraintDef);
                    _primaryKeys.AddRange(columns);
                }
                else if (constraintDef.StartsWith("FOREIGN KEY"))
                {
                    // Parse FOREIGN KEY constraint
                    // Pattern: FOREIGN KEY (`col1`, `col2`) REFERENCES `catalog`.`schema`.`table` (`refcol1`, `refcol2`)
                    var fkPattern = @"FOREIGN KEY\s*\((.+?)\)\s*REFERENCES\s+`([^`]+)`\.`([^`]+)`\.`([^`]+)`\s*\((.+)\)";
                    var fkMatch = Regex.Match(constraintDef, fkPattern);

                    if (fkMatch.Success)
                    {
                        var localColumnsPart = fkMatch.Groups[1].Value;
                        var refCatalog = fkMatch.Groups[2].Value;
                        var refSchema = fkMatch.Groups[3].Value;
                        var refTable = fkMatch.Groups[4].Value;
                        var refColumnsPart = fkMatch.Groups[5].Value;

                        var localColumns = ExtractColNames(localColumnsPart);
                        var refColumns = ExtractColNames(refColumnsPart);

                        _foreignKeys.Add(new ForeignKeyInfo
                        {
                            LocalColumns = localColumns,
                            RefColumns = refColumns,
                            RefCatalog = refCatalog,
                            RefSchema = refSchema,
                            RefTable = refTable
                        });
                    }
                }
            }
        }

        /// <summary>
        /// Represents a parsed constraint with name and definition
        /// </summary>
        private class ParsedConstraint
        {
            public string Name { get; set; } = String.Empty;
            public string Definition { get; set; } = String.Empty;
        }

        /// <summary>
        /// Parses the constraint list string, properly handling backtick-quoted identifiers with special characters
        /// </summary>
        /// <param name="input">The inner content of the constraints (without outer brackets)</param>
        /// <returns>List of parsed constraints</returns>
        private List<ParsedConstraint> ParseConstraintList(string input)
        {
            var constraints = new List<ParsedConstraint>();
            var i = 0;
            
            while (i < input.Length)
            {
                // Skip whitespace
                while (i < input.Length && char.IsWhiteSpace(input[i])) i++;
                
                // Should be at opening parenthesis of constraint
                if (i >= input.Length || input[i] != '(') break;
                
                i++; // Skip opening parenthesis
                
                // Parse constraint name (everything until first comma)
                var nameStart = i;
                while (i < input.Length && input[i] != ',') i++;
                
                if (i >= input.Length) break;
                
                var constraintName = input.Substring(nameStart, i - nameStart).Trim();
                i++; // Skip comma
                
                // Skip whitespace after comma
                while (i < input.Length && char.IsWhiteSpace(input[i])) i++;
                
                // Parse constraint definition (everything until matching closing parenthesis)
                var definitionStart = i;
                var parenDepth = 0;
                var inBackticks = false;
                
                while (i < input.Length)
                {
                    var c = input[i];
                    
                    if (c == '`')
                    {
                        // Check for escaped backtick (double backtick)
                        if (i + 1 < input.Length && input[i + 1] == '`')
                        {
                            // Skip both backticks - this is an escaped backtick
                            i += 2;
                            continue;
                        }
                        else
                        {
                            // Toggle backtick state
                            inBackticks = !inBackticks;
                        }
                    }
                    else if (!inBackticks)
                    {
                        if (c == '(')
                        {
                            parenDepth++;
                        }
                        else if (c == ')')
                        {
                            if (parenDepth == 0)
                            {
                                // This is the closing parenthesis for the constraint
                                break;
                            }
                            parenDepth--;
                        }
                    }
                    
                    i++;
                }
                
                if (i >= input.Length) break;
                
                var constraintDef = input.Substring(definitionStart, i - definitionStart).Trim();
                i++; // Skip closing parenthesis
                
                constraints.Add(new ParsedConstraint
                {
                    Name = constraintName,
                    Definition = constraintDef
                });
                
                // Skip whitespace and optional comma
                while (i < input.Length && (char.IsWhiteSpace(input[i]) || input[i] == ',')) i++;
            }
            
            return constraints;
        }

        /// <summary>
        /// Extracts column names enclosed in backticks from a string.
        /// </summary>
        /// <param name="input">Input string containing backtick-quoted identifiers</param>
        /// <returns>List of extracted column names</returns>
        private List<string> ExtractColNames(string input)
        {
            var identifiers = new List<string>();
            var i = 0;
            
            while (i < input.Length)
            {
                // Find the start of a backtick-quoted identifier
                if (input[i] == '`')
                {
                    var start = i + 1; // Start after opening backtick
                    i++; // Skip opening backtick
                    
                    // Find the closing backtick, handling escaped backticks
                    while (i < input.Length)
                    {
                        if (input[i] == '`')
                        {
                            // Check for escaped backtick (double backtick)
                            if (i + 1 < input.Length && input[i + 1] == '`')
                            {
                                // Skip both backticks for escaped backtick
                                i += 2;
                            }
                            else
                            {
                                // Found closing backtick
                                break;
                            }
                        }
                        else
                        {
                            i++;
                        }
                    }
                    
                    if (i < input.Length && input[i] == '`')
                    {
                        // Extract the identifier content and handle escaped backticks
                        var rawIdentifier = input.Substring(start, i - start);
                        var processedIdentifier = rawIdentifier.Replace("``", "`");
                        identifiers.Add(processedIdentifier);
                        i++; // Skip closing backtick
                    }
                }
                else
                {
                    i++;
                }
            }
            
            return identifiers;
        }
    }
}
