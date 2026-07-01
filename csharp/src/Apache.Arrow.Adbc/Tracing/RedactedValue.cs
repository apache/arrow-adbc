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

using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Tracing
{
    /// <summary>
    /// Stores a value that should be redacted when converted to string or serialized to JSON.
    /// The value can still be retrieved using the GetValue method.
    /// </summary>
    /// <param name="value"></param>
    [JsonConverter(typeof(ToStringJsonConverter<RedactedValue>))]
    public class RedactedValue(object? value)
    {
        private readonly object? _value = value;

        public const string DefaultValue = "[REDACTED]";

        /// <summary>
        /// Returns a string representation of the redacted value. This will always return the default value <see cref="DefaultValue"/>
        /// regardless of the actual value stored in the object.
        /// </summary>
        /// <returns></returns>
        public override string ToString() => DefaultValue;

        /// <summary>
        /// Gets the actual value stored in the object. This method can be used to retrieve the original value if needed,
        /// but it should be used with caution as it may contain sensitive information.
        /// </summary>
        /// <returns></returns>
        public object? GetValue()
        {
            return _value;
        }
    }
}
