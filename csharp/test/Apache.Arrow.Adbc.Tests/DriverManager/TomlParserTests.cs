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
    }
}
