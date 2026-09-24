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

using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Apache.Arrow.Adbc.Telemetry.Traces.Listeners.FileListener;
using Apache.Arrow.Adbc.Tracing;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Telemetry.Traces.Listeners.FileListener
{
    public class SerializableActivityTests
    {
        private readonly ITestOutputHelper _output;

        public class SerializableActivityTestData : TheoryData<Activity>
        {
            public SerializableActivityTestData()
            {
                var activityWithTags = new Activity("TestActivityWithTags");
                int index = 0;

                activityWithTags.AddTag("key" + index++, "value1");
                activityWithTags.AddTag("key" + index++, (sbyte)123);
                activityWithTags.AddTag("key" + index++, (byte)123);
                activityWithTags.AddTag("key" + index++, (short)123);
                activityWithTags.AddTag("key" + index++, (ushort)123);
                activityWithTags.AddTag("key" + index++, (int)123);
                activityWithTags.AddTag("key" + index++, (uint)123);
                activityWithTags.AddTag("key" + index++, (long)123);
                activityWithTags.AddTag("key" + index++, (ulong)123);
                activityWithTags.AddTag("key" + index++, (float)123);
                activityWithTags.AddTag("key" + index++, (double)123);
                activityWithTags.AddTag("key" + index++, (decimal)123);
                activityWithTags.AddTag("key" + index++, true);
                activityWithTags.AddTag("key" + index++, 'A');

                activityWithTags.AddTag("key" + index++, new string[] { "val1" });
                activityWithTags.AddTag("key" + index++, new byte[] { 123 });
                activityWithTags.AddTag("key" + index++, new sbyte[] { 123 });
                activityWithTags.AddTag("key" + index++, new ushort[] { 123 });
                activityWithTags.AddTag("key" + index++, new short[] { 123 });
                activityWithTags.AddTag("key" + index++, new uint[] { 123 });
                activityWithTags.AddTag("key" + index++, new int[] { 123 });
                activityWithTags.AddTag("key" + index++, new ulong[] { 123 });
                activityWithTags.AddTag("key" + index++, new long[] { 123 });
                activityWithTags.AddTag("key" + index++, new float[] { 123 });
                activityWithTags.AddTag("key" + index++, new double[] { 123 });
                activityWithTags.AddTag("key" + index++, new decimal[] { 123 });
                activityWithTags.AddTag("key" + index++, new bool[] { true });
                activityWithTags.AddTag("key" + index++, new char[] { 'A' });

                activityWithTags.AddTag("key" + index++, new string[] { "val1" }.AsEnumerable());
                activityWithTags.AddTag("key" + index++, new byte[] { 123 }.AsEnumerable());
                activityWithTags.AddTag("key" + index++, new sbyte[] { 123 }.AsEnumerable());
                activityWithTags.AddTag("key" + index++, new ushort[] { 123 }.AsEnumerable());
                activityWithTags.AddTag("key" + index++, new short[] { 123 }.AsEnumerable());
                activityWithTags.AddTag("key" + index++, new uint[] { 123 }.AsEnumerable());
                activityWithTags.AddTag("key" + index++, new int[] { 123 }.AsEnumerable());
                activityWithTags.AddTag("key" + index++, new ulong[] { 123 }.AsEnumerable());
                activityWithTags.AddTag("key" + index++, new long[] { 123 }.AsEnumerable());
                activityWithTags.AddTag("key" + index++, new float[] { 123 }.AsEnumerable());
                activityWithTags.AddTag("key" + index++, new double[] { 123 }.AsEnumerable());
                activityWithTags.AddTag("key" + index++, new decimal[] { 123 }.AsEnumerable());
                activityWithTags.AddTag("key" + index++, new bool[] { true }.AsEnumerable());
                activityWithTags.AddTag("key" + index++, new char[] { 'A' }.AsEnumerable());

                activityWithTags.AddTag("key" + index++, new Uri("http://example.com"));
                Add(activityWithTags);
            }
        }

        public SerializableActivityTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task CanRedactValue()
        {
            string activityName = NewName();
            Activity activity = new Activity(activityName).Start();
            using ActivityWithPii? activityWithPii = ActivityWithPii.New(activity);
            Assert.NotNull(activityWithPii);
            string testValue = NewName();
            activityWithPii.AddTag("keyName", new RedactedValue(testValue));
            var value = activity.TagObjects.First().Value as RedactedValue;
            Assert.NotNull(value);
            Assert.Equal("[REDACTED]", value.ToString());
            Assert.Equal(testValue, value.GetValue());

            SerializableActivity serializableActivity = new(activity);
            var stream = new MemoryStream();
            await JsonSerializer.SerializeAsync(
                stream,
                serializableActivity);
            Assert.NotNull(stream);
            string actualString = Encoding.UTF8.GetString(stream.ToArray());
            Assert.DoesNotContain(testValue, actualString);
            Assert.Contains("[REDACTED]", actualString);
        }

        [Fact]
        public async Task CanUnredactValue()
        {
            string activityName = NewName();
            Activity activity = new Activity(activityName).Start();
            using ActivityWithPii? activityWithPii = ActivityWithPii.New(activity);
            Assert.NotNull(activityWithPii);
            activity.Start();
            string testValue = NewName();
            activityWithPii.AddTag("keyName", new RedactedValue(testValue));
            var value = activity.TagObjects.First().Value as RedactedValue;
            Assert.NotNull(value);
            Assert.Equal("[REDACTED]", value.ToString());
            Assert.Equal(testValue, value.GetValue());

            SerializableActivity serializableActivity = new(activity);
            var stream = new MemoryStream();
            JsonSerializerOptions serializerOptions = new()
            {
                Converters =
                {
                    new UnredactConverter()
                },
            };
            await JsonSerializer.SerializeAsync(
                stream,
                serializableActivity,
                serializerOptions);
            Assert.NotNull(stream);
            string actualString = Encoding.UTF8.GetString(stream.ToArray());
            Assert.Contains(testValue, actualString);
            Assert.DoesNotContain("[REDACTED]", actualString);
        }

        private string NewName() => Guid.NewGuid().ToString("N");
    }
}
