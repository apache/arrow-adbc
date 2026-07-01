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

namespace Apache.Arrow.Adbc.Tracing
{
    /// <summary>
    /// Converts an object to a JSON string using its ToString() method.
    /// This is useful for types that do not have a default JSON converter
    /// or when you want to control how the object is represented in JSON.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ToStringJsonConverter<T> : JsonConverter<T>
    {
        public override T? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            throw new NotImplementedException();

        public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions _)
        {
            writer.WriteStringValue(value?.ToString());
        }
    }
}
