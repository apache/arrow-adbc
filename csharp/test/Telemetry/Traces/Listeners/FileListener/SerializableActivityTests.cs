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
        public async Task CannnotSerializeAnonymousObjectWithSerializerContext()
        {
            Activity activity = new Activity("activity");
            using (activity.Start())
            {
                activity.AddTag("key1", new { Field1 = "value1" });
                SerializableActivity serializableActivity = new(activity);
                var stream = new MemoryStream();
                var serializerOptions = new JsonSerializerOptions
                {
                    WriteIndented = true,
                    IncludeFields = true,
                    TypeInfoResolver = SerializableActivitySerializerContext.Default,
                };
                await Assert.ThrowsAnyAsync<Exception>(async () => await JsonSerializer.SerializeAsync(
                    stream,
                    serializableActivity,
                    serializerOptions));
            }
        }

        [Theory]
        [ClassData(typeof(SerializableActivityTestData))]
        public async Task CanSerializeWithNoDefaultTypeInfoResolver(Activity activity)
        {
            using (activity.Start())
            {
                SerializableActivity serializableActivity = new(activity);
                var stream = new MemoryStream();
                var serializerOptions = new JsonSerializerOptions
                {
                    WriteIndented = true,
                    IncludeFields = true,
                    TypeInfoResolver = SerializableActivitySerializerContext.Default,
                };
                await JsonSerializer.SerializeAsync(
                    stream,
                    serializableActivity,
                    serializerOptions);
                Assert.NotNull(stream);
                _output.WriteLine("Serialized Activity: {0}", Encoding.UTF8.GetString(stream.ToArray()));
            }
        }

        [Theory]
        [ClassData(typeof(SerializableActivityTestData))]
        public async Task CanSerializeWithDefaultTypeInfoResolver(Activity activity)
        {
            using (activity.Start())
            {
                SerializableActivity serializableActivity = new(activity);
                var stream = new MemoryStream();
                var serializerOptions = new JsonSerializerOptions
                {
                    WriteIndented = true,
                    IncludeFields = true,
                    TypeInfoResolver = JsonTypeInfoResolver.Combine(
                        SerializableActivitySerializerContext.Default,
                        new DefaultJsonTypeInfoResolver()),
                };
                await JsonSerializer.SerializeAsync(
                    stream,
                    serializableActivity,
                    serializerOptions);
                Assert.NotNull(stream);
                _output.WriteLine("Serialized Activity: {0}", Encoding.UTF8.GetString(stream.ToArray()));
            }
        }

        [Fact]
        public async Task CanSerializeAnonymousObjectWithDefaultTypeInfoResolver()
        {
            Activity activity = new Activity("activity");
            using (activity.Start())
            {
                activity.AddTag("key1", new { Field1 = "value1" });
                SerializableActivity serializableActivity = new(activity);
                var stream = new MemoryStream();
                var serializerOptions = new JsonSerializerOptions
                {
                    WriteIndented = true,
                    IncludeFields = true,
                    TypeInfoResolver = JsonTypeInfoResolver.Combine(
                        SerializableActivitySerializerContext.Default,
                        new DefaultJsonTypeInfoResolver()),
                };
                await JsonSerializer.SerializeAsync(
                    stream,
                    serializableActivity,
                    serializerOptions);
                _output.WriteLine("Serialized Activity: {0}", Encoding.UTF8.GetString(stream.ToArray()));
            }
        }
    }
}
