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
using Apache.Arrow.Adbc.DriverManager;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.DriverManager
{
    /// <summary>
    /// Tests for <see cref="TomlParser"/> section-header validation.
    /// </summary>
    public class TomlParserTests
    {
        [Theory]
        [InlineData("[ADBC]")]
        [InlineData("[ADBC.features]")]
        [InlineData("[Driver]")]
        [InlineData("[Driver.shared]")]
        [InlineData("[my-section_1]")]
        public void Parse_AcceptsManifestSpecSectionNames(string header)
        {
            string content = header + "\nkey = \"value\"\n";

            Dictionary<string, Dictionary<string, object>> result = TomlParser.Parse(content);

            string sectionName = header.Substring(1, header.Length - 2);
            Assert.True(result.ContainsKey(sectionName));
            Assert.Equal("value", result[sectionName]["key"]);
        }

        [Theory]
        [InlineData("[foo bar]")]   // whitespace inside segment
        [InlineData("[\"quoted\"]")] // quoted name
        [InlineData("[a..b]")]       // empty dotted segment
        [InlineData("[.foo]")]       // leading dot
        [InlineData("[foo.]")]       // trailing dot
        [InlineData("[]")]            // empty name
        [InlineData("[foo/bar]")]   // disallowed character
        [InlineData("[foo:bar]")]   // disallowed character
        public void Parse_RejectsInvalidSectionNames(string header)
        {
            string content = header + "\nkey = \"value\"\n";

            Assert.Throws<FormatException>(() => TomlParser.Parse(content));
        }

        [Theory]
        [InlineData("key = \"\"\"foo\"\"\"")]   // multi-line basic string
        [InlineData("key = '''foo'''")]         // multi-line literal string
        [InlineData("key = { a = 1 }")]         // inline table
        [InlineData("key = 1_000")]             // underscored integer
        [InlineData("key = 0xff")]              // hex integer
        [InlineData("key = 0o77")]              // octal integer
        [InlineData("key = 0b10")]              // binary integer
        [InlineData("key = 2024-01-01")]        // date
        [InlineData("key = bareword")]           // bare unquoted string
        [InlineData("key = \"unterminated")]   // unterminated double-quoted string
        [InlineData("key = 'unterminated")]    // unterminated single-quoted string
        [InlineData("key = TRUE")]               // non-lowercase boolean
        [InlineData("key = False")]              // non-lowercase boolean
        [InlineData("key = \"foo\"trailing")]  // trailing content after basic string
        [InlineData("key = 'foo'trailing")]    // trailing content after literal string
        [InlineData("key = \"bad\\xescape\"")] // unsupported basic-string escape
        [InlineData("key = [[1, 2], [3]]")]   // nested arrays
        [InlineData("key = [{ a = 1 }]")]      // array containing inline table
        [InlineData("key = [,1]")]              // leading comma / empty element
        [InlineData("key = [1,,2]")]            // double comma / empty element
        [InlineData("key = [")]                  // multi-line / unterminated array
        public void Parse_RejectsUnsupportedValueProductions(string line)
        {
            Assert.Throws<FormatException>(() => TomlParser.Parse(line + "\n"));
        }

        [Theory]
        [InlineData("[foo")]                    // missing closing bracket
        [InlineData("key value")]               // missing '='
        [InlineData("= value")]                 // missing key
        [InlineData("\"quoted-key\" = 1")]    // quoted keys are not supported
        [InlineData("key with spaces = 1")]    // whitespace-containing keys are not supported
        public void Parse_RejectsMalformedLines(string line)
        {
            Assert.Throws<FormatException>(() => TomlParser.Parse(line + "\n"));
        }

        [Fact]
        public void Parse_AcceptsDotNamespacedKeyAsFlatKey()
        {
            // ADBC connection profiles use dot-namespaced option names as flat keys.
            // The Rust and C++ driver managers reach the same result by parsing them
            // as nested tables and then flattening; this minimal parser accepts them
            // as bare keys directly. The dictionary entry must be the literal dotted
            // string -- not nested -- so it can be passed to AdbcDatabaseSetOption.
            Dictionary<string, Dictionary<string, object>> result =
                TomlParser.Parse("adbc.snowflake.sql.warehouse = \"COMPUTE_WH\"\n");
            Assert.Equal("COMPUTE_WH", result[""]["adbc.snowflake.sql.warehouse"]);
        }

        [Theory]
        [InlineData("key = \"hello\"", "hello")]
        [InlineData("key = \"\"", "")]
        [InlineData("key = \"say \\\"hi\\\"\"", "say \"hi\"")]
        public void Parse_AcceptsDoubleQuotedStrings(string line, string expected)
        {
            Dictionary<string, Dictionary<string, object>> result = TomlParser.Parse(line + "\n");
            Assert.Equal(expected, result[""]["key"]);
        }

        [Theory]
        [InlineData("key = 'hello'", "hello")]
        [InlineData("key = ''", "")]
        [InlineData("key = 'Driver Display Name'", "Driver Display Name")]
        [InlineData("key = 'C:\\\\path\\\\to\\\\driver.dll'", "C:\\\\path\\\\to\\\\driver.dll")]
        public void Parse_AcceptsSingleQuotedLiteralStrings(string line, string expected)
        {
            Dictionary<string, Dictionary<string, object>> result = TomlParser.Parse(line + "\n");
            Assert.Equal(expected, result[""]["key"]);
        }

        [Fact]
        public void Parse_LiteralString_PreservesHashCharacter()
        {
            // A '#' inside a literal string must not be treated as a comment marker.
            Dictionary<string, Dictionary<string, object>> result =
                TomlParser.Parse("key = 'has#hash' # actual comment\n");
            Assert.Equal("has#hash", result[""]["key"]);
        }

        [Fact]
        public void Parse_LiteralString_BackslashesAreLiteral()
        {
            // In TOML literal strings, backslashes are taken literally with no escape
            // processing -- this is the standard idiom for Windows paths in manifests.
            Dictionary<string, Dictionary<string, object>> result =
                TomlParser.Parse(@"key = 'C:\path\to\driver.dll'" + "\n");
            Assert.Equal(@"C:\path\to\driver.dll", result[""]["key"]);
        }

        [Fact]
        public void Parse_AcceptsEmptyArray()
        {
            Dictionary<string, Dictionary<string, object>> result =
                TomlParser.Parse("supported = []\n");
            List<object> arr = Assert.IsType<List<object>>(result[""]["supported"]);
            Assert.Empty(arr);
        }

        [Fact]
        public void Parse_AcceptsArrayOfStrings()
        {
            Dictionary<string, Dictionary<string, object>> result =
                TomlParser.Parse("features = ['bulk insert', \"async\"]\n");
            List<object> arr = Assert.IsType<List<object>>(result[""]["features"]);
            Assert.Equal(new object[] { "bulk insert", "async" }, arr);
        }

        [Fact]
        public void Parse_AcceptsArrayOfMixedScalars()
        {
            Dictionary<string, Dictionary<string, object>> result =
                TomlParser.Parse("mixed = [1, 2.5, true, 'x']\n");
            List<object> arr = Assert.IsType<List<object>>(result[""]["mixed"]);
            Assert.Equal(new object[] { 1L, 2.5, true, "x" }, arr);
        }

        [Fact]
        public void Parse_AcceptsTrailingCommaInArray()
        {
            Dictionary<string, Dictionary<string, object>> result =
                TomlParser.Parse("arr = ['a', 'b',]\n");
            List<object> arr = Assert.IsType<List<object>>(result[""]["arr"]);
            Assert.Equal(new object[] { "a", "b" }, arr);
        }

        [Fact]
        public void Parse_Array_HashInsideStringElementIsLiteral()
        {
            // The strip-comment pass must respect strings inside array bodies too.
            Dictionary<string, Dictionary<string, object>> result =
                TomlParser.Parse("arr = ['a#b', 'c'] # comment\n");
            List<object> arr = Assert.IsType<List<object>>(result[""]["arr"]);
            Assert.Equal(new object[] { "a#b", "c" }, arr);
        }

        [Fact]
        public void Parse_AcceptsRecognizedScalarValues()
        {
            string content = "s = \"x\"\nlit = 'y'\ni = 42\nf = 3.14\nb1 = true\nb2 = false\n";

            Dictionary<string, Dictionary<string, object>> result = TomlParser.Parse(content);

            Assert.Equal("x", result[""]["s"]);
            Assert.Equal("y", result[""]["lit"]);
            Assert.Equal(42L, result[""]["i"]);
            Assert.Equal(3.14, result[""]["f"]);
            Assert.Equal(true, result[""]["b1"]);
            Assert.Equal(false, result[""]["b2"]);
        }

        [Fact]
        public void Parse_DriverManifestExample_ParsesAllFields()
        {
            // This is the canonical ADBC driver-manifest layout from the spec docs.
            // Combines literal strings, integers, empty arrays, inline comments, and
            // both top-level and dotted section headers -- if any of those regress,
            // this test catches it.
            const string toml = @"manifest_version = 1

name = 'Driver Display Name'
version = '1.0.0' # driver version
publisher = 'string to identify the publisher'
license = 'Apache-2.0' # or otherwise
url = 'https://example.com' # URL with more info about the driver
                            # such as a github link or documentation.

[ADBC]
version = '1.1.0' # Maximum supported ADBC spec version

[ADBC.features]
supported = [] # list of strings such as 'bulk insert'
unsupported = [] # list of strings such as 'async'

[Driver]
entrypoint = 'AdbcDriverInit' # entrypoint to use if not using default
# You can provide just a single path
# shared = '/path/to/libadbc_driver.so'

# or you can provide platform-specific paths for scenarios where the driver
# is distributed with multiple platforms supported by a single package.
[Driver.shared]
# paths to shared libraries to load based on platform tuple
linux_amd64 = '/path/to/libadbc_driver.so'
osx_amd64 = '/path/to/libadbc_driver.dylib'
windows_amd64 = 'C:\\path\\to\\adbc_driver.dll'
";
            Dictionary<string, Dictionary<string, object>> result = TomlParser.Parse(toml);

            Assert.Equal(1L, result[""]["manifest_version"]);
            Assert.Equal("Driver Display Name", result[""]["name"]);
            Assert.Equal("1.0.0", result[""]["version"]);
            Assert.Equal("string to identify the publisher", result[""]["publisher"]);
            Assert.Equal("Apache-2.0", result[""]["license"]);
            Assert.Equal("https://example.com", result[""]["url"]);

            Assert.Equal("1.1.0", result["ADBC"]["version"]);

            List<object> supported = Assert.IsType<List<object>>(result["ADBC.features"]["supported"]);
            List<object> unsupported = Assert.IsType<List<object>>(result["ADBC.features"]["unsupported"]);
            Assert.Empty(supported);
            Assert.Empty(unsupported);

            Assert.Equal("AdbcDriverInit", result["Driver"]["entrypoint"]);

            Assert.Equal("/path/to/libadbc_driver.so", result["Driver.shared"]["linux_amd64"]);
            Assert.Equal("/path/to/libadbc_driver.dylib", result["Driver.shared"]["osx_amd64"]);
            // Literal strings preserve backslashes verbatim: '\\' in source means two
            // literal backslash characters in the parsed value.
            Assert.Equal(@"C:\\path\\to\\adbc_driver.dll", result["Driver.shared"]["windows_amd64"]);
        }
    }
}
