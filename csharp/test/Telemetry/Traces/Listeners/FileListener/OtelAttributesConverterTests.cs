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
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using Apache.Arrow.Adbc.Telemetry.Traces.Listeners.FileListener;

namespace Apache.Arrow.Adbc.Tests.Telemetry.Traces.Listeners.FileListener
{
    /// <summary>
    /// Verifies that <see cref="OtelAttributeWriter"/> emits OTel-compatible types as native
    /// JSON scalars/arrays and falls back to an invariant-culture string for everything else.
    /// </summary>
    public class OtelAttributesConverterTests
    {
        private static string WriteDict(IReadOnlyDictionary<string, object?> dict)
        {
            var converter = new OtelAttributesDictionaryConverter();
            using var ms = new MemoryStream();
            using (var writer = new Utf8JsonWriter(ms))
            {
                converter.Write(writer, dict, new JsonSerializerOptions());
            }
            return Encoding.UTF8.GetString(ms.ToArray());
        }

        private static string WriteList(IReadOnlyList<KeyValuePair<string, object?>> list)
        {
            var converter = new OtelAttributesListConverter();
            using var ms = new MemoryStream();
            using (var writer = new Utf8JsonWriter(ms))
            {
                converter.Write(writer, list, new JsonSerializerOptions());
            }
            return Encoding.UTF8.GetString(ms.ToArray());
        }

        [Fact]
        public void StringValueIsNativeString()
            => Assert.Equal("{\"k\":\"v\"}", WriteDict(new Dictionary<string, object?> { ["k"] = "v" }));

        [Fact]
        public void BoolValueIsNativeBoolean()
            => Assert.Equal("{\"k\":true}", WriteDict(new Dictionary<string, object?> { ["k"] = true }));

        [Fact]
        public void Int32ValueIsNativeNumber()
            => Assert.Equal("{\"k\":42}", WriteDict(new Dictionary<string, object?> { ["k"] = 42 }));

        [Fact]
        public void Int64ValueIsNativeNumber()
            => Assert.Equal("{\"k\":5000000000}", WriteDict(new Dictionary<string, object?> { ["k"] = 5_000_000_000L }));

        [Fact]
        public void DoubleValueIsNativeNumber()
            => Assert.Equal("{\"k\":1.5}", WriteDict(new Dictionary<string, object?> { ["k"] = 1.5 }));

        [Fact]
        public void NullValueIsJsonNull()
            => Assert.Equal("{\"k\":null}", WriteDict(new Dictionary<string, object?> { ["k"] = null }));

        [Fact]
        public void StringArrayIsJsonArrayOfStrings()
            => Assert.Equal("{\"k\":[\"a\",\"b\"]}", WriteDict(new Dictionary<string, object?> { ["k"] = new[] { "a", "b" } }));

        [Fact]
        public void BoolArrayIsJsonArrayOfBooleans()
            => Assert.Equal("{\"k\":[true,false]}", WriteDict(new Dictionary<string, object?> { ["k"] = new[] { true, false } }));

        [Fact]
        public void Int64ArrayIsJsonArrayOfNumbers()
            => Assert.Equal("{\"k\":[1,2,3]}", WriteDict(new Dictionary<string, object?> { ["k"] = new long[] { 1, 2, 3 } }));

        [Fact]
        public void DoubleArrayIsJsonArrayOfNumbers()
            => Assert.Equal("{\"k\":[1.5,2.5]}", WriteDict(new Dictionary<string, object?> { ["k"] = new[] { 1.5, 2.5 } }));

        [Fact]
        public void DateTimeFallsBackToInvariantString()
        {
            // Explicitly run under a comma-as-decimal locale to prove the output is invariant.
            var prev = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = new CultureInfo("de-DE");
                var dt = new DateTime(2026, 4, 18, 12, 34, 56, DateTimeKind.Utc);
                string actual = WriteDict(new Dictionary<string, object?> { ["k"] = dt });
                // IFormattable.ToString(null, invariant) for DateTime uses the invariant
                // general format: "04/18/2026 12:34:56".
                Assert.Equal("{\"k\":\"04/18/2026 12:34:56\"}", actual);
            }
            finally
            {
                CultureInfo.CurrentCulture = prev;
            }
        }

        [Fact]
        public void DecimalFallsBackToInvariantNumericString()
        {
            var prev = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = new CultureInfo("de-DE");
                // decimal isn't in the OTel spec — falls to invariant string. Period, not comma.
                string actual = WriteDict(new Dictionary<string, object?> { ["k"] = 1.5m });
                Assert.Equal("{\"k\":\"1.5\"}", actual);
            }
            finally
            {
                CultureInfo.CurrentCulture = prev;
            }
        }

        [Fact]
        public void ListConverterWritesArrayOfKeyValueObjects()
        {
            var list = new[]
            {
                new KeyValuePair<string, object?>("a", 1),
                new KeyValuePair<string, object?>("b", "two"),
                new KeyValuePair<string, object?>("c", true),
            };
            Assert.Equal("[{\"Key\":\"a\",\"Value\":1},{\"Key\":\"b\",\"Value\":\"two\"},{\"Key\":\"c\",\"Value\":true}]", WriteList(list));
        }

        [Fact]
        public void ReadThrows()
        {
            var json = "{\"k\":1}";
            byte[] bytes = Encoding.UTF8.GetBytes(json);
            var options = new JsonSerializerOptions();
            Assert.Throws<NotSupportedException>(() =>
            {
                var reader = new Utf8JsonReader(bytes);
                new OtelAttributesDictionaryConverter().Read(ref reader, typeof(IReadOnlyDictionary<string, object?>), options);
            });
        }
    }
}
