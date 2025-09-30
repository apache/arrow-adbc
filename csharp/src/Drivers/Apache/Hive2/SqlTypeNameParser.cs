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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    /// <summary>
    /// Interface for the SQL type name parser.
    /// </summary>
    internal interface ISqlTypeNameParser
    {
        /// <summary>
        /// Tries to parse the input string for a valid SQL type definition.
        /// </summary>
        /// <param name="input">The SQL type definition string to parse.</param>
        /// <param name="result">If successful, the result; otherwise <c>null</c>.</param>
        /// <returns>True if it can successfully parse the type definition input string; otherwise false.</returns>
        bool TryParse(string input, out SqlTypeNameParserResult? result);
    }

    /// <summary>
    /// Abstract and generic SQL data type name parser.
    /// </summary>
    /// <typeparam name="T">The <see cref="SqlTypeNameParserResult"/> type when returning a successful parse</typeparam>
    internal abstract class SqlTypeNameParser<T> : ISqlTypeNameParser where T : SqlTypeNameParserResult
    {
        private static readonly ConcurrentDictionary<string, SqlTypeNameParserResult> s_cache = new();

        private static readonly IReadOnlyDictionary<int, ISqlTypeNameParser> s_parserMap = new Dictionary<int, ISqlTypeNameParser>()
        {
            { (int)HiveServer2Connection.ColumnTypeId.ARRAY, SqlArrayTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.BIGINT, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.BIGINT.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.BIT, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.BIT.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.BINARY, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.BINARY.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.BLOB, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.BLOB.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.BOOLEAN, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.BOOLEAN.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.CHAR, SqlCharTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.CLOB, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.CLOB.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.DATALINK, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.DATALINK.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.DATE, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.DATE.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.DECIMAL, SqlDecimalTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.DISTINCT, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.DISTINCT.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.DOUBLE, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.DOUBLE.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.FLOAT, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.FLOAT.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.INTEGER, SqlIntegerTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.JAVA_OBJECT, SqlMapTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.LONGNVARCHAR, SqlVarcharTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.LONGVARCHAR, SqlVarcharTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.NCHAR, SqlCharTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.NCLOB, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.NCLOB.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.NULL, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.NULL.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.NUMERIC, SqlDecimalTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.NVARCHAR, SqlVarcharTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.OTHER, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.OTHER.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.REAL, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.REAL.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.REF_CURSOR, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.REF_CURSOR.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.REF, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.REF.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.ROWID, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.ROWID.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.SMALLINT, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.SMALLINT.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.STRUCT, SqlStructTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.TIME, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.TIME.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.TIME_WITH_TIMEZONE, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.TIME_WITH_TIMEZONE.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.TIMESTAMP, SqlTimestampTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.TIMESTAMP_WITH_TIMEZONE, SqlTimestampTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.TINYINT, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.TINYINT.ToString()) },
            { (int)HiveServer2Connection.ColumnTypeId.VARCHAR, SqlVarcharTypeParser.Default },
            { (int)HiveServer2Connection.ColumnTypeId.SQLXML, SqlSimpleTypeParser.Default(HiveServer2Connection.ColumnTypeId.SQLXML.ToString()) },
        };

        // Note: the INTERVAL sql type does not have an associated column type id.
        private static readonly HashSet<ISqlTypeNameParser> s_parsers = new HashSet<ISqlTypeNameParser>(s_parserMap.Values
            .Concat([
                SqlIntervalTypeParser.Default,
                SqlSimpleTypeParser.Default("VOID"),
                SqlSimpleTypeParser.Default("VARIANT"),
            ]));

        /// <summary>
        /// Gets the base SQL type name without decoration or sub clauses
        /// </summary>
        public abstract string BaseTypeName { get; }

        /// <summary>
        /// Parses the input type name string and produces a result.
        /// When a matching parser is found that successfully parses the type name string, the result of that parse is returned.
        /// If no parser is able to successfully match the input type name,
        /// then a <see cref="NotSupportedException"/> is thrown.
        /// </summary>
        /// <param name="input">The type name string to parse</param>
        /// <param name="columnTypeIdHint">If provided, the column type id is used as a hint to find the most likely matching parser.</param>
        /// <returns>
        /// A parser result, from a successful match and parse.
        /// </returns>
        public static T Parse(string input, int? columnTypeIdHint = null) =>
            SqlTypeNameParser<T>.TryParse(input, out SqlTypeNameParserResult? result, columnTypeIdHint) && result != null
                ? CastResultOrThrow(input, result)
                : throw new NotSupportedException($"Unsupported SQL type name: '{input}'");

        /// <summary>
        /// Gets the <see cref="Regex"/> expression to parse the SQL type name
        /// </summary>
        protected abstract Regex Expression { get; }

        /// <summary>
        /// Generates the successful result for a matching parse
        /// </summary>
        /// <param name="input">The original SQL type name</param>
        /// <param name="match">The successful <see cref="Match"/> result</param>
        /// <returns></returns>
        protected virtual T GenerateResult(string input, Match match) => (T)new SqlTypeNameParserResult(input, BaseTypeName);

        private static T CastResultOrThrow(string input, SqlTypeNameParserResult result) =>
            result is T typedResult
                ? typedResult
                : throw new InvalidCastException($"Cannot cast return type '{result.GetType().Name}' to type '{typeof(T).Name}' for input SQL type name: '{input}'.");

        /// <summary>
        /// Tries to parse the input string for a valid SQL type definition.
        /// </summary>
        /// <param name="input">The SQL type definition string to parse.</param>
        /// <param name="result">If successful, the result; otherwise <c>null</c>.</param>
        /// <returns>True if it can successfully parse the type definition input string; otherwise false.</returns>
        bool ISqlTypeNameParser.TryParse(string input, out SqlTypeNameParserResult? result)
        {
            bool success = TryParse(input, out T? typedResult);
            result = success ? typedResult : (SqlTypeNameParserResult?)default;
            return success;
        }

        /// <summary>
        /// Tries to parse the input string for a valid SQL type definition.
        /// </summary>
        /// <param name="input">The SQL type definition string to parse.</param>
        /// <param name="result">If successful, the result; otherwise <c>null</c>.</param>
        /// <returns>True if it can successfully parse the type definition input string; otherwise false.</returns>
        internal bool TryParse(string input, out T? result)
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
        /// Tries to parse the input SQL type name. If a matching parser is found and can parse the type name, it's result is set in <c>parserResult</c> and <c>true</c> is returned.
        /// If a matching parser is not found <c>parserResult</c> is set to null and <c>false</c> is returned.
        /// </summary>
        /// <param name="input">The SQL type name to parse</param>
        /// <param name="parserResult">The result of a successful parse, <c>null</c> otherwise</param>
        /// <param name="columnTypeIdHint">The column type id as a hint to find the most appropriate parser</param>
        /// <returns><c>true</c> if a matching parser is able to parse the SQL type name, <c>false</c> otherwise</returns>
        internal static bool TryParse(string input, out SqlTypeNameParserResult? parserResult, int? columnTypeIdHint = null)
        {
            // Note: there may be multiple calls that successfully add/set the value in the cache
            // - but the parser will produce the same result in each case.
            string trimmedInput = input.Trim();
            if (s_cache.ContainsKey(trimmedInput))
            {
                parserResult = s_cache[trimmedInput];
                return true;
            }

            ISqlTypeNameParser? sqlTypeNameParser = null;
            if (columnTypeIdHint != null && s_parserMap.ContainsKey(columnTypeIdHint.Value))
            {
                sqlTypeNameParser = s_parserMap[columnTypeIdHint.Value];
                if (sqlTypeNameParser.TryParse(input, out SqlTypeNameParserResult? result) && result != null)
                {
                    parserResult = result;
                    s_cache[trimmedInput] = result;
                    return true;
                }
            }
            foreach (ISqlTypeNameParser parser in s_parsers)
            {
                if (parser == sqlTypeNameParser) continue;
                if (parser.TryParse(input, out SqlTypeNameParserResult? result) && result != null)
                {
                    parserResult = result;
                    s_cache[trimmedInput] = result;
                    return true;
                }
            }

            parserResult = null;
            return false;
        }
    }

    /// <summary>
    /// A result for parsing a SQL data type.
    /// </summary>
    /// <param name="typeName">The original SQL type name to parse</param>
    /// <param name="baseTypeName">The 'base' type name to use which is typically more simple without sub-clauses</param>
    internal class SqlTypeNameParserResult(string typeName, string baseTypeName)
    {
        /// <summary>
        /// The original SQL type name
        /// </summary>
        public string TypeName { get; } = typeName;

        /// <summary>
        /// The 'base' type name to use which is typically more simple without sub-clauses
        /// </summary>
        public string BaseTypeName { get; } = baseTypeName;

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(this, obj)) return true;
            if (obj is not SqlTypeNameParserResult other) return false;
            return TypeName.Equals(other.TypeName)
                && BaseTypeName.Equals(other.BaseTypeName);
        }

        public override int GetHashCode()
        {
            return TypeName.GetHashCode() ^ BaseTypeName.GetHashCode();
        }
    }

    /// <summary>
    /// An result for parsing the SQL CHAR/NCHAR/STRING/VARCHAR/NVARCHAR/LONGVARCHAR/LONGNVARCHAR data types.
    /// </summary>
    /// <param name="typeName">The original SQL type name to parse</param>
    /// <param name="baseTypeName">The 'base' type name without the length clause</param>
    /// <param name="columnSize">The length of the column for this type name</param>
    internal class SqlCharVarcharParserResult(string typeName, string baseTypeName, int columnSize = SqlVarcharTypeParser.VarcharColumnSizeDefault) : SqlTypeNameParserResult(typeName, baseTypeName)
    {
        /// <summary>
        /// The length of the column for this type name
        /// </summary>
        public int ColumnSize { get; } = columnSize;

        public override bool Equals(object? obj) => obj is SqlCharVarcharParserResult result
            && base.Equals(obj)
            && TypeName == result.TypeName
            && BaseTypeName == result.BaseTypeName
            && ColumnSize == result.ColumnSize;

        public override int GetHashCode() => base.GetHashCode()
            ^ TypeName.GetHashCode()
            ^ BaseTypeName.GetHashCode()
            ^ ColumnSize.GetHashCode();
    }

    /// <summary>
    /// An result for parsing the SQL DECIMAL/DEC/NUMERIC data types.
    /// </summary>
    /// <param name="typeName">The original SQL type name to parse</param>
    /// <param name="baseTypeName">The 'base' type name without the precision or scale clause</param>
    /// <param name="precision">The precision of the decimal type</param>
    /// <param name="scale">The scale (decimal digits) of the decimal type</param>
    internal class SqlDecimalParserResult(string typeName, string baseTypeName, int precision, int scale) : SqlTypeNameParserResult(typeName, baseTypeName)
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

        public override bool Equals(object? obj) => obj is SqlDecimalParserResult result
                && base.Equals(obj)
                && TypeName == result.TypeName
                && BaseTypeName == result.BaseTypeName
                && Precision == result.Precision
                && Scale == result.Scale
                && EqualityComparer<Decimal128Type>.Default.Equals(Decimal128Type, result.Decimal128Type);

        public override int GetHashCode() => base.GetHashCode()
            ^ TypeName.GetHashCode()
            ^ BaseTypeName.GetHashCode()
            ^ Precision.GetHashCode()
            ^ Scale.GetHashCode()
            ^ Decimal128Type.GetHashCode();
    }

    internal class SqlIntervalParserResult(string typeName, string baseTypeName, string qualifiers) : SqlTypeNameParserResult(typeName, baseTypeName)
    {
        public string Qualifiers { get; } = qualifiers;

        public override bool Equals(object? obj) => obj is SqlIntervalParserResult result
                && base.Equals(obj)
                && TypeName == result.TypeName
                && BaseTypeName == result.BaseTypeName
                && Qualifiers == result.Qualifiers;

        public override int GetHashCode() => base.GetHashCode()
            ^ TypeName.GetHashCode()
            ^ BaseTypeName.GetHashCode()
            ^ Qualifiers.GetHashCode();
    }

    /// <summary>
    /// Provides a parser for CHAR type definitions.
    /// </summary>
    internal class SqlCharTypeParser : SqlTypeNameParser<SqlCharVarcharParserResult>
    {
        public static SqlCharTypeParser Default { get; } = new();

        public override string BaseTypeName => "CHAR";

        private const int CharColumnSizeDefault = 255;

        // Allow precision definition to be optional
        private static readonly Regex s_expression = new(
            @"^\s*(?<typeName>((CHAR)|(NCHAR)))(\s*\(\s*(?<precision>\d{1,10})\s*\))?\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        protected override Regex Expression => s_expression;

        protected override SqlCharVarcharParserResult GenerateResult(string input, Match match)
        {
            GroupCollection groups = match.Groups;
            Group precisionGroup = groups["precision"];

            int precision = int.TryParse(precisionGroup.Value, out int candidatePrecision)
                ? candidatePrecision
                : CharColumnSizeDefault;
            return new SqlCharVarcharParserResult(input, BaseTypeName, precision);
        }
    }

    /// <summary>
    /// Provides a parser for SQL VARCHAR/STRING type definitions.
    /// </summary>
    internal class SqlVarcharTypeParser : SqlTypeNameParser<SqlCharVarcharParserResult>
    {
        internal const int VarcharColumnSizeDefault = int.MaxValue;
        private const string StringBaseTypeName = "STRING";

        public static SqlVarcharTypeParser Default => new();

        public override string BaseTypeName => "VARCHAR";

        private static readonly Regex s_expression = new(
            @"^\s*(?<typeName>((STRING)|(VARCHAR)|(LONGVARCHAR)|(LONGNVARCHAR)|(NVARCHAR)))(\s*\(\s*(?<precision>\d{1,10})\s*\))?\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        protected override Regex Expression => s_expression;

        protected override SqlCharVarcharParserResult GenerateResult(string input, Match match)
        {
            GroupCollection groups = match.Groups;
            Group precisionGroup = groups["precision"];
            Group typeNameGroup = groups["typeName"];

            string baseTypeName = typeNameGroup.Value.Equals(StringBaseTypeName, StringComparison.InvariantCultureIgnoreCase)
                ? StringBaseTypeName
                : BaseTypeName;
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

        public static SqlDecimalTypeParser Default => new();

        public override string BaseTypeName => "DECIMAL";

        // Pattern is based on this definition
        // https://docs.databricks.com/en/sql/language-manual/data-types/decimal-type.html#syntax
        // { DECIMAL | DEC | NUMERIC } [ (  p [ , s ] ) ]
        // p: Optional maximum result (total number of digits) of the number between 1 and 38. The default is 10.
        // s: Optional scale of the number between 0 and p. The number of digits to the right of the decimal point. The default is 0.
        private static readonly Regex s_expression = new(
            @"^\s*(?<typeName>((DECIMAL)|(DEC)|(NUMERIC)))(\s*\(\s*((?<precision>\d{1,2})(\s*\,\s*(?<scale>\d{1,2}))?)\s*\))?\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        protected override Regex Expression => s_expression;

        protected override SqlDecimalParserResult GenerateResult(string input, Match match)
        {
            GroupCollection groups = match.Groups;
            Group precisionGroup = groups["precision"];
            Group scaleGroup = groups["scale"];

            int precision = precisionGroup.Success && int.TryParse(precisionGroup.Value, out int candidatePrecision) ? candidatePrecision : DecimalPrecisionDefault;
            int scale = scaleGroup.Success && int.TryParse(scaleGroup.Value, out int candidateScale) ? candidateScale : DecimalScaleDefault;

            return new SqlDecimalParserResult(input, BaseTypeName, precision, scale);
        }
    }

    /// <summary>
    /// Provides a parser for SQL INTEGER type definitions.
    /// </summary>
    internal class SqlIntegerTypeParser : SqlTypeNameParser<SqlTypeNameParserResult>
    {
        public static SqlIntegerTypeParser Default => new();

        public override string BaseTypeName => "INTEGER";

        // Pattern is based on this definition
        // https://docs.databricks.com/en/sql/language-manual/data-types/int-type.html#syntax
        // { INT | INTEGER }
        private static readonly Regex s_expression = new(
            @"^\s*(?<typeName>((INTEGER)|(INT)))\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        protected override Regex Expression => s_expression;
    }

    /// <summary>
    /// Provides a parser for SQL TIMESTAMP type definitions.
    /// </summary>
    internal class SqlTimestampTypeParser : SqlTypeNameParser<SqlTypeNameParserResult>
    {
        public static SqlTimestampTypeParser Default => new();

        public override string BaseTypeName => "TIMESTAMP";

        // Pattern is based on this definition
        // https://docs.databricks.com/en/sql/language-manual/data-types/map-type.html#syntax
        // MAP <keyType, valueType>
        // keyType: Any data type other than MAP specifying the keys.
        // valueType: Any data type specifying the values.
        private static readonly Regex s_expression = new(
            @"^\s*(?<typeName>((TIMESTAMP)|(TIMESTAMP_LTZ)|(TIMESTAMP_NTZ)))\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        protected override Regex Expression => s_expression;
    }

    /// <summary>
    /// Provides a parser for SQL STRUCT type definitions.
    /// </summary>
    internal class SqlStructTypeParser : SqlTypeNameParser<SqlTypeNameParserResult>
    {
        public static SqlStructTypeParser Default => new();

        public override string BaseTypeName => "STRUCT";

        // Pattern is based on this definition
        // https://docs.databricks.com/en/sql/language-manual/data-types/struct-type.html#syntax
        // STRUCT < [fieldName [:] fieldType [NOT NULL] [COMMENT str] [, …] ] >
        // fieldName: An identifier naming the field. The names need not be unique.
        // fieldType: Any data type.
        // NOT NULL: When specified the struct guarantees that the value of this field is never NULL.
        // COMMENT str: An optional string literal describing the field.
        private static readonly Regex s_expression = new(
            @"^\s*(?<typeName>STRUCT)(?<structClause>\s*\<(.+)\>)\s*$", // STRUCT
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        protected override Regex Expression => s_expression;
    }

    /// <summary>
    /// Provides a parser for SQL ARRAY type definitions.
    /// </summary>
    internal class SqlArrayTypeParser : SqlTypeNameParser<SqlTypeNameParserResult>
    {
        public static SqlArrayTypeParser Default => new();

        public override string BaseTypeName => "ARRAY";

        // Pattern is based on this definition
        // https://docs.databricks.com/en/sql/language-manual/data-types/array-type.html#syntax
        // ARRAY < elementType >
        // elementType: Any data type defining the type of the elements of the array.
        private static readonly Regex s_expression = new(
            @"^\s*(?<typeName>ARRAY)(?<arrayClause>\s*\<(.+)\>)\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        protected override Regex Expression => s_expression;
    }

    /// <summary>
    /// Provides a parser for SQL MAP type definitions.
    /// </summary>
    internal class SqlMapTypeParser : SqlTypeNameParser<SqlTypeNameParserResult>
    {
        public static SqlMapTypeParser Default => new();

        public override string BaseTypeName => "MAP";

        // Pattern is based on this definition
        // https://docs.databricks.com/en/sql/language-manual/data-types/map-type.html#syntax
        // MAP <keyType, valueType>
        // keyType: Any data type other than MAP specifying the keys.
        // valueType: Any data type specifying the values.
        private static readonly Regex s_expression = new(
            @"^\s*(?<typeName>MAP)(?<mapClause>\s*\<(.+)\>)\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        protected override Regex Expression => s_expression;
    }

    internal class SqlIntervalTypeParser : SqlTypeNameParser<SqlTypeNameParserResult>
    {
        public static SqlIntervalTypeParser Default => new();

        public override string BaseTypeName { get; } = "INTERVAL";

        // See: https://docs.databricks.com/en/sql/language-manual/data-types/interval-type.html#syntax
        private static readonly Regex s_expression = new(
            @"^\s*(?<typeName>INTERVAL)\s+.*$",
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        protected override Regex Expression => s_expression;
    }

    internal class SqlSimpleTypeParser(string baseTypeName) : SqlTypeNameParser<SqlTypeNameParserResult>
    {
        private static readonly ConcurrentDictionary<string, SqlSimpleTypeParser> s_parserMap = new ConcurrentDictionary<string, SqlSimpleTypeParser>();

        public static SqlSimpleTypeParser Default(string baseTypeName)
        {
            return s_parserMap.GetOrAdd(baseTypeName, (typeName) => new SqlSimpleTypeParser(typeName));
        }

        public override string BaseTypeName { get; } = baseTypeName;

        protected override Regex Expression => new(
            @"^\s*" + Regex.Escape(BaseTypeName) + @"\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);
    }
}
