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
using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Arrow.Adbc.Tracing;

namespace Apache.Arrow.Adbc.Telemetry.Traces.Listeners.FileListener
{
    public class UnredactConverter : JsonConverter<RedactedValue>
    {
        public override bool CanConvert(Type typeToConvert) => typeToConvert == typeof(RedactedValue);
        public override RedactedValue? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => throw new NotImplementedException();
        public override void Write(Utf8JsonWriter writer, RedactedValue value, JsonSerializerOptions options)
        {
            writer.WriteRawValue(JsonSerializer.Serialize(value.GetValue(), options));
        }
    }
}
