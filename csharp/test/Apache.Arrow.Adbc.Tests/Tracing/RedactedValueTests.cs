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
using System.Text.Json;
using Apache.Arrow.Adbc.Tracing;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Tracing
{
    public class RedactedValueTests
    {
        [Fact]
        internal void CanCreateRedactedValue()
        {
            // Arrange
            string originalValue = Guid.NewGuid().ToString("N");
            var redacted = new RedactedValue(originalValue);
            Assert.True(redacted.GetIsPii());
            Assert.Equal(RedactedValue.RedactedValueDefault, redacted.Value);
            Assert.Equal(originalValue, redacted.GetUnredactedValue());
        }

        [Fact]
        internal void CanCreateRedactedValueNoPii()
        {
            // Arrange
            string originalValue = Guid.NewGuid().ToString("N");
            var redacted = new RedactedValue(originalValue, isPii: false);
            Assert.False(redacted.GetIsPii());
            Assert.Equal(originalValue, redacted.Value);
            Assert.Equal(originalValue, redacted.GetUnredactedValue());
        }

        [Theory]
        [MemberData(nameof(GenerateSerializationData))]
        internal void CanSerializeCreateRedactedValue(object? originalValue, bool isPii)
        {
            var redacted = new RedactedValue(originalValue, isPii);
            string serialized = JsonSerializer.Serialize(redacted);
            if (!isPii || originalValue == null)
            {
                Assert.DoesNotContain(RedactedValue.RedactedValueDefault, serialized);
                if (originalValue != null)
                {
                    Assert.Contains(JsonSerializer.Serialize(originalValue), serialized);
                }
                else
                {
                    Assert.Contains("null", serialized);
                }
            }
            else
            {
                Assert.Contains(RedactedValue.RedactedValueDefault, serialized);
                Assert.DoesNotContain(JsonSerializer.Serialize(originalValue), serialized);
            }
        }

        public static IEnumerable<object?[]> GenerateSerializationData()
        {
            foreach (var isPii in new[] { true, false })
            {
                yield return new object?[] { null, isPii };
                yield return new object?[] { "secret", isPii };
                yield return new object?[] { 5, isPii };
                yield return new object?[] { new[] { 1, 2 }, isPii };
            }
        }
    }
}
