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
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    public class SqlTypeNameParserTests(ITestOutputHelper outputHelper)
    {
        private readonly ITestOutputHelper _outputHelper = outputHelper;

        [Theory()]
        [InlineData("ARRAY<INT>", "ARRAY")]
        [InlineData("ARRAY < INT >", "ARRAY")]
        [InlineData(" ARRAY < ARRAY < INT > > ", "ARRAY")]
        [InlineData("ARRAY<VARCHAR(255)>", "ARRAY")]
        [InlineData("DATE", "DATE")]
        [InlineData("dec(15)", "DECIMAL")]
        [InlineData("numeric", "DECIMAL")]
        [InlineData("STRUCT<F1:INT>", "STRUCT")]
        [InlineData("STRUCT< F1 INT >", "STRUCT")]
        [InlineData("STRUCT < F1: ARRAY < INT > > ", "STRUCT")]
        [InlineData("STRUCT<F1: VARCHAR(255), F2 ARRAY<STRING>>", "STRUCT")]
        [InlineData("MAP<INT,STRING>", "MAP")]
        [InlineData("MAP< INT , VARCHAR(255) >", "MAP")]
        [InlineData("MAP < ARRAY < INT >, INT > ", "MAP")]
        [InlineData("TIMESTAMP", "TIMESTAMP")]
        [InlineData("TIMESTAMP_LTZ", "TIMESTAMP")]
        [InlineData("TIMESTAMP_NTZ", "TIMESTAMP")]
        internal void CanParseAnyType(string testTypeName, string expectedBaseTypeName)
        {
            SqlTypeNameParserResult result = SqlTypeNameParser<SqlTypeNameParserResult>.Parse(testTypeName);
            Assert.NotNull(result);
            Assert.Equal(testTypeName, result.TypeName);
            Assert.Equal(expectedBaseTypeName, result.BaseTypeName);
        }

        [Theory()]
        [InlineData("BIGINT", "BIGINT")]
        [InlineData("BINARY", "BINARY")]
        [InlineData("BOOLEAN", "BOOLEAN")]
        [InlineData("DATE", "DATE")]
        [InlineData("DOUBLE", "DOUBLE")]
        [InlineData("FLOAT", "FLOAT")]
        [InlineData("SMALLINT", "SMALLINT")]
        [InlineData("TINYINT", "TINYINT")]
        internal void CanParseSimpleTypeName(string testTypeName, string expectedBaseTypeName)
        {
            Assert.True(SqlTypeNameParser<SqlTypeNameParserResult>.TryParse(testTypeName, out SqlTypeNameParserResult? result));
            Assert.NotNull(result);
            Assert.Equal(expectedBaseTypeName, result.BaseTypeName);
        }

        [Theory()]
        [InlineData("INTERVAL YEAR", "INTERVAL")]
        [InlineData("INTERVAL MONTH", "INTERVAL")]
        [InlineData("INTERVAL DAY", "INTERVAL")]
        [InlineData("INTERVAL HOUR", "INTERVAL")]
        [InlineData("INTERVAL MINUTE", "INTERVAL")]
        [InlineData("INTERVAL SECOND", "INTERVAL")]
        [InlineData("INTERVAL YEAR TO MONTH", "INTERVAL")]
        [InlineData("INTERVAL DAY TO HOUR", "INTERVAL")]
        [InlineData("INTERVAL DAY TO MINUTE", "INTERVAL")]
        [InlineData("INTERVAL DAY TO SECOND", "INTERVAL")]
        [InlineData("INTERVAL HOUR TO MINUTE", "INTERVAL")]
        [InlineData("INTERVAL HOUR TO SECOND", "INTERVAL")]
        [InlineData("INTERVAL MINUTE TO SECOND", "INTERVAL")]
        internal void CanParseInterval(string testTypeName, string expectedBaseTypeName)
        {
            Assert.True(SqlTypeNameParser<SqlTypeNameParserResult>.TryParse(testTypeName, out SqlTypeNameParserResult? result));
            Assert.NotNull(result);
            Assert.Equal(expectedBaseTypeName, result.BaseTypeName);
        }

        [Theory()]
        [MemberData(nameof(GenerateCharTestData), "CHAR")]
        [MemberData(nameof(GenerateCharTestData), "NCHAR")]
        [MemberData(nameof(GenerateCharTestData), "CHaR")]
        internal void CanParseChar(string testTypeName, SqlCharVarcharParserResult expected)
        {
            _outputHelper.WriteLine(testTypeName);
            Assert.True(SqlCharTypeParser.Default.TryParse(testTypeName, out SqlCharVarcharParserResult? result));
            Assert.NotNull(result);
            Assert.Equal(expected, result);
        }

        [Theory()]
        [MemberData(nameof(GenerateVarcharTestData), "VARCHAR")]
        [MemberData(nameof(GenerateVarcharTestData), "LONGVARCHAR")]
        [MemberData(nameof(GenerateVarcharTestData), "NVARCHAR")]
        [MemberData(nameof(GenerateVarcharTestData), "LONGNVARCHAR")]
        [MemberData(nameof(GenerateVarcharTestData), "VaRCHaR")]
        internal void CanParseVarchar(string testTypeName, SqlCharVarcharParserResult expected)
        {
            _outputHelper.WriteLine(testTypeName);
            Assert.True(SqlVarcharTypeParser.Default.TryParse(testTypeName, out SqlCharVarcharParserResult? result));
            Assert.NotNull(result);
            Assert.Equal(expected, result);
        }

        [Theory()]
        [MemberData(nameof(GenerateDecimalTestData), "DECIMAL")]
        [MemberData(nameof(GenerateDecimalTestData), "DEC")]
        [MemberData(nameof(GenerateDecimalTestData), "NUMERIC")]
        [MemberData(nameof(GenerateDecimalTestData), "DeCiMaL")]
        internal void CanParseDecimal(string testTypeName, SqlDecimalParserResult expected)
        {
            _outputHelper.WriteLine(testTypeName);
            Assert.True(SqlDecimalTypeParser.Default.TryParse(testTypeName, out SqlDecimalParserResult? result));
            Assert.NotNull(result);
            Assert.Equal(expected.TypeName, result.TypeName);
            Assert.Equal(expected.BaseTypeName, result.BaseTypeName);
            // Note: Decimal128Type does not override Equals/GetHashCode
            Assert.Equal(expected.Decimal128Type.Name, result.Decimal128Type.Name);
            Assert.Equal(expected.Decimal128Type.Precision, result.Decimal128Type.Precision);
            Assert.Equal(expected.Decimal128Type.Scale, result.Decimal128Type.Scale);
        }

        [Theory()]
        [InlineData("INT")]
        [InlineData("INTEGER")]
        [InlineData(" INT ")]
        [InlineData(" INTEGER ")]
        [InlineData(" iNTeGeR ")]
        internal void CanParseInteger(string testTypeName)
        {
            string baseTypeName = SqlIntegerTypeParser.Default.BaseTypeName;
            SqlTypeNameParserResult expected = new(testTypeName, baseTypeName);
            _outputHelper.WriteLine(testTypeName);
            Assert.True(SqlIntegerTypeParser.Default.TryParse(testTypeName, out SqlTypeNameParserResult? result));
            Assert.NotNull(result);
            Assert.Equal(expected, result);
        }

        [Theory()]
        [InlineData("TIMESTAMP")]
        [InlineData("TIMESTAMP_LTZ")]
        [InlineData("TIMESTAMP_NTZ")]
        [InlineData("TiMeSTaMP")]
        internal void CanParseTimestamp(string testTypeName)
        {
            string baseTypeName = SqlTimestampTypeParser.Default.BaseTypeName;
            SqlTypeNameParserResult expected = new(testTypeName, baseTypeName);
            _outputHelper.WriteLine(testTypeName);
            Assert.True(SqlTimestampTypeParser.Default.TryParse(testTypeName, out SqlTypeNameParserResult? result));
            Assert.NotNull(result);
            Assert.Equal(expected, result);
        }

        [Theory()]
        [InlineData("ARRAY<INT>")]
        [InlineData("ARRAY < INT >")]
        [InlineData(" ARRAY < ARRAY < INT > > ")]
        [InlineData("ARRAY<VARCHAR(255)>")]
        [InlineData("aRRaY<iNT>")]
        internal void CanParseArray(string testTypeName)
        {
            string baseTypeName = SqlArrayTypeParser.Default.BaseTypeName;
            SqlTypeNameParserResult expected = new(testTypeName, baseTypeName);
            _outputHelper.WriteLine(testTypeName);
            Assert.True(SqlArrayTypeParser.Default.TryParse(testTypeName, out SqlTypeNameParserResult? result));
            Assert.NotNull(result);
            Assert.Equal(expected, result);
        }

        [Theory()]
        [InlineData("MAP<INT,STRING>")]
        [InlineData("MAP< INT , VARCHAR(255) >")]
        [InlineData("MAP < ARRAY < INT >, INT > ")]
        [InlineData("MaP<iNT,STRiNG>")]
        internal void CanParseMap(string testTypeName)
        {
            string baseTypeName = SqlMapTypeParser.Default.BaseTypeName;
            SqlTypeNameParserResult expected = new(testTypeName, baseTypeName);
            _outputHelper.WriteLine(testTypeName);
            Assert.True(SqlMapTypeParser.Default.TryParse(testTypeName, out SqlTypeNameParserResult? result));
            Assert.NotNull(result);
            Assert.Equal(expected, result);
        }

        [Theory()]
        [InlineData("STRUCT<F1:INT>")]
        [InlineData("STRUCT< F1 INT >")]
        [InlineData("STRUCT < F1: ARRAY < INT > > ")]
        [InlineData("STRUCT<F1: VARCHAR(255), F2 ARRAY<STRING>>")]
        [InlineData("STRuCT<F1:iNT>")]
        internal void CanParseStruct(string testTypeName)
        {
            string baseTypeName = SqlStructTypeParser.Default.BaseTypeName;
            SqlTypeNameParserResult expected = new(testTypeName, baseTypeName);
            _outputHelper.WriteLine(testTypeName);
            Assert.True(SqlStructTypeParser.Default.TryParse(testTypeName, out SqlTypeNameParserResult? result));
            Assert.NotNull(result);
            Assert.Equal(expected, result);
        }

        [Theory()]
        [InlineData("ARRAY")]
        [InlineData("MAP")]
        [InlineData("STRUCT")]
        [InlineData("ARRAY<")]
        [InlineData("MAP<")]
        [InlineData("STRUCT<")]
        [InlineData("ARRAY>")]
        [InlineData("MAP>")]
        [InlineData("STRUCT>")]
        [InlineData("INTERVAL")]
        [InlineData("TIMESTAMP_ZZZ")]
        internal void CannotParseUnexpectedTypeName(string testTypeName)
        {
            Assert.False(SqlTypeNameParser<SqlTypeNameParserResult>.TryParse(testTypeName, out _), $"Expecting type {testTypeName} to fail to parse.");
        }

        [Fact()]
        internal void CanDetectInvalidReturnType()
        {
            Func<object?> testCode = () => SqlTypeNameParser<SqlDecimalParserResult>.Parse("INTEGER", (int)HiveServer2Connection.ColumnTypeId.INTEGER);
            _outputHelper.WriteLine(Assert.Throws<InvalidCastException>(testCode).Message);
        }

        public static IEnumerable<object[]> GenerateCharTestData(string typeName)
        {
            int?[] lengths = [1, 10, int.MaxValue,];
            string[] spaces = ["", " ", "\t"];
            string baseTypeName = SqlCharTypeParser.Default.BaseTypeName;
            foreach (int? length in lengths)
            {
                foreach (string leadingSpace in spaces)
                {
                    foreach (string trailingSpace in spaces)
                    {
                        string clause = length == null ? "" : $"{leadingSpace}({leadingSpace}{length}{trailingSpace})";
                        string testTypeName = $"{leadingSpace}{typeName}{clause}{trailingSpace}";
                        SqlCharVarcharParserResult expectedResult = new(testTypeName, baseTypeName, length ?? int.MaxValue);
                        yield return new object[] { testTypeName, expectedResult };
                    }
                }
            }
        }

        public static IEnumerable<object[]> GenerateVarcharTestData(string typeName)
        {
            int?[] lengths = [null, 1, 10, int.MaxValue,];
            string[] spaces = ["", " ", "\t"];
            string baseTypeName = SqlVarcharTypeParser.Default.BaseTypeName;
            foreach (int? length in lengths)
            {
                foreach (string leadingSpace in spaces)
                {
                    foreach (string trailingSpace in spaces)
                    {
                        string clause = length == null ? "" : $"{leadingSpace}({leadingSpace}{length}{trailingSpace})";
                        string testTypeName = $"{leadingSpace}{typeName}{clause}{trailingSpace}";
                        SqlCharVarcharParserResult expectedResult = new(testTypeName, baseTypeName, length ?? int.MaxValue);
                        yield return new object[] { testTypeName, expectedResult };
                    }
                }
            }
            yield return new object[] { "STRING", new SqlCharVarcharParserResult("STRING", "STRING") };
        }

        public static IEnumerable<object[]> GenerateDecimalTestData(string typeName)
        {
            string baseTypeName = SqlDecimalTypeParser.Default.BaseTypeName;
            var precisionScales = new[]
            {
                new { Precision = (int?)null, Scale = (int?)null },
                new { Precision = (int?)1, Scale = (int?)null },
                new { Precision = (int?)1, Scale = (int?)1 },
                new { Precision = (int?)38, Scale = (int?)null },
                new { Precision = (int?)38, Scale = (int?)38 },
                new { Precision = (int?)99, Scale = (int?)null },
                new { Precision = (int?)99, Scale = (int?)99 },
            };
            string[] spaces = ["", " ", "\t"];
            foreach (var precisionScale in precisionScales)
            {
                foreach (string leadingSpace in spaces)
                {
                    foreach (string trailingSpace in spaces)
                    {
                        string clause = precisionScale.Precision == null ? ""
                            : precisionScale.Scale == null
                                ? $"({leadingSpace}{precisionScale.Precision}{trailingSpace})"
                                : $"({leadingSpace}{precisionScale.Precision}{trailingSpace},{leadingSpace}{precisionScale.Scale}{trailingSpace})";
                        string testTypeName = $"{leadingSpace}{typeName}{clause}{trailingSpace}";
                        SqlDecimalParserResult expectedResult = new(testTypeName, baseTypeName, precisionScale.Precision ?? 10, precisionScale.Scale ?? 0);
                        yield return new object[] { testTypeName, expectedResult };
                    }
                }
            }
        }
    }
}
