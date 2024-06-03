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
using System.Text.RegularExpressions;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    /// <summary>
    /// Abstract and generic SQL data type name parser.
    /// </summary>
    /// <typeparam name="T">The <see cref="ParserResult"/> type when returning a successful parse</typeparam>
    internal abstract class SqlTypeNameParser<T> where T : ParserResult
    {
        /// <summary>
        /// Gets the <see cref="Regex"/> expression to parse the SQL type name
        /// </summary>
        public abstract Regex Expression { get; }

        /// <summary>
        /// Generates the successful result of a matching parse
        /// </summary>
        /// <param name="input">The original SQL type name</param>
        /// <param name="match">The successful <see cref="Match"/> result</param>
        /// <returns></returns>
        public abstract T GenerateResult(string input, Match match);

        /// <summary>
        /// Tries to parse the input string for a valid SQL type definition.
        /// </summary>
        /// <param name="input">The SQL type defintion string to parse.</param>
        /// <param name="result">If successful, the result; otherwise <c>null</c>.</param>
        /// <returns>True if it can successfully parse the type definition input string; otherwise false.</returns>
        public bool TryParse(string input, out T? result)
        {
            Match match = Expression.Match(input);
            if (!match.Success)
            {
                result = default;
                return false;
            }

            result = GenerateResult(input, match);
            return match.Success;
        }

        /// <summary>
        /// Parses the input string for a valid SQL type definition and returns the result or returns the <c>defaultValue</c>, if invalid.
        /// </summary>
        /// <param name="input">The SQL type defintion string to parse.</param>
        /// <param name="defaultValue">If input string is an invalid type definition, this result is returned instead.</param>
        /// <returns>If input string is a valid SQL type definition, it returns the result; otherwise <c>defaultValue</c>.</returns>
        public T ParseOrDefault(string input, T defaultValue)
        {
            return TryParse(input, out T? result) ? result! : defaultValue;
        }
    }

    /// <summary>
    /// An result for parsing a SQL data type.
    /// </summary>
    /// <param name="typeName">The original SQL type name to parse</param>
    /// <param name="baseTypeName">The 'base' type name to use which is typically more simple without sub-clauses</param>
    internal class ParserResult(string typeName, string baseTypeName)
    {
        /// <summary>
        /// The original SQL type name
        /// </summary>
        public string TypeName { get; } = typeName;

        /// <summary>
        /// The 'base' type name to use which is typically more simple without sub-clauses
        /// </summary>
        public string BaseTypeName { get; } = baseTypeName;
    }

    /// <summary>
    /// An result for parsing the SQL CHAR/NCHAR/STRING/VARCHAR/NVARCHAR/LONGVARCHAR/LONGNVARCHAR data types.
    /// </summary>
    /// <param name="typeName">The original SQL type name to parse</param>
    /// <param name="baseTypeName">The 'base' type name without the length clause</param>
    /// <param name="columnSize">The length of the column for this type name</param>
    internal class SqlCharVarcharParserResult(string typeName, string baseTypeName, int columnSize) : ParserResult(typeName, baseTypeName)
    {
        /// <summary>
        /// The length of the column for this type name
        /// </summary>
        public int ColumnSize { get; } = columnSize;
    }

    /// <summary>
    /// An result for parsing the SQL DECIMAL/DEC/NUMERIC data types.
    /// </summary>
    /// <param name="typeName">The original SQL type name to parse</param>
    /// <param name="baseTypeName">The 'base' type name without the precision or scale clause</param>
    /// <param name="precision">The precision of the decimal type</param>
    /// <param name="scale">The scale (decimal digits) of the decimal type</param>
    internal class SqlDecimalParserResult(string typeName, string baseTypeName, int precision, int scale) : ParserResult(typeName, baseTypeName)
    {
        /// <summary>
        /// Constructs a new default result given the original type name.
        /// </summary>
        /// <param name="typeName">The original SQL type name to parse</param>
        public SqlDecimalParserResult(string typeName) : this(typeName, "DECIMAL", SqlDecimalTypeParser.DecimalPrecisionDefault, SqlDecimalTypeParser.DecimalScaleDefault) { }

        /// <summary>
        /// The precision of the decimal type
        /// </summary>
        public int Precision { get; } = precision;

        /// <summary>
        /// The scale (decimal digits) of the decimal type
        /// </summary>
        public int Scale { get; } = scale;

        /// <summary>
        /// The <see cref='Types.Decimal128Type'/> representing the parsed type name
        /// </summary>
        public Decimal128Type Decimal128Type { get; } = new Decimal128Type(precision, scale);
    }

    /// <summary>
    /// Provides a parser for CHAR type definitions.
    /// </summary>
    internal class SqlCharTypeParser : SqlTypeNameParser<SqlCharVarcharParserResult>
    {
        private const string BaseTypeName = "CHAR";

        private static readonly Regex s_expression = new(
            @"^\s*(?<typeName>((CHAR)|(NCHAR)))(\s*\(\s*(?<precision>\d{1,10})\s*\))\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        public override Regex Expression => s_expression;

        public override SqlCharVarcharParserResult GenerateResult(string input, Match match)
        {
            GroupCollection groups = match.Groups;
            Group precisionGroup = groups["precision"];

            int precision = int.TryParse(precisionGroup.Value, out int candidatePrecision)
                ? candidatePrecision
                : throw new ArgumentException($"Unable to parse length: '{precisionGroup.Value}'", nameof(input));
            return new SqlCharVarcharParserResult(input, BaseTypeName, precision);
        }
    }

    /// <summary>
    /// Provides a parser for SQL VARCHAR/STRING type definitions.
    /// </summary>
    internal class SqlVarcharTypeParser : SqlTypeNameParser<SqlCharVarcharParserResult>
    {
        internal const int VarcharColumnSizeDefault = int.MaxValue;

        private const string VarcharBaseTypeName = "VARCHAR";
        private const string StringBaseTypeName = "STRING";

        private static readonly Regex s_expression = new(
            @"^\s*(?<typeName>((STRING)|(VARCHAR)|(LONGVARCHAR)|(LONGNVARCHAR)|(NVARCHAR)))(\s*\(\s*(?<precision>\d{1,10})\s*\))?\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        public override Regex Expression => s_expression;

        public override SqlCharVarcharParserResult GenerateResult(string input, Match match)
        {
            GroupCollection groups = match.Groups;
            Group precisionGroup = groups["precision"];
            Group typeNameGroup = groups["typeName"];

            string baseTypeName = typeNameGroup.Value.Equals(StringBaseTypeName, StringComparison.InvariantCultureIgnoreCase)
                ? StringBaseTypeName
                : VarcharBaseTypeName;
            int precision = precisionGroup.Success && int.TryParse(precisionGroup.Value, out int candidatePrecision)
                ? candidatePrecision
                : VarcharColumnSizeDefault;
            return new SqlCharVarcharParserResult(input, baseTypeName, precision);
        }
    }

    /// <summary>
    /// Provides a parser for SQL DECIMAL type definitions.
    /// </summary>
    internal class SqlDecimalTypeParser : SqlTypeNameParser<SqlDecimalParserResult>
    {
        internal const int DecimalPrecisionDefault = 10;
        internal const int DecimalScaleDefault = 0;

        private const string BaseTypeName = "DECIMAL";

        // Pattern is based on this definition
        // https://docs.databricks.com/en/sql/language-manual/data-types/decimal-type.html#syntax
        // { DECIMAL | DEC | NUMERIC } [ (  p [ , s ] ) ]
        // p: Optional maximum result (total number of digits) of the number between 1 and 38. The default is 10.
        // s: Optional scale of the number between 0 and p. The number of digits to the right of the decimal point. The default is 0.
        private static readonly Regex s_expression = new(
            @"^\s*(?<typeName>((DECIMAL)|(DEC)|(NUMERIC)))(\s*\(\s*((?<precision>\d{1,2})(\s*\,\s*(?<scale>\d{1,2}))?)\s*\))?\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        public override Regex Expression => s_expression;

        public override SqlDecimalParserResult GenerateResult(string input, Match match)
        {
            GroupCollection groups = match.Groups;
            Group precisionGroup = groups["precision"];
            Group scaleGroup = groups["scale"];

            int precision = precisionGroup.Success && int.TryParse(precisionGroup.Value, out int candidatePrecision) ? candidatePrecision : DecimalPrecisionDefault;
            int scale = scaleGroup.Success && int.TryParse(scaleGroup.Value, out int candidateScale) ? candidateScale : DecimalScaleDefault;

            return new SqlDecimalParserResult(input, BaseTypeName, precision, scale);
        }
    }
}
