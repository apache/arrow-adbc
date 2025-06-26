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
using System.Diagnostics;
using System.Text;

namespace Apache.Arrow.Adbc.Tracing
{
    public static class ActivityExtensions
    {
        /// <summary>
        /// Add a new <see cref="ActivityEvent"/> object to the <see cref="Activity.Events" /> list.
        /// </summary>
        /// <param name="eventName">The name of the event.</param>
        /// <param name="tags">The optional list of tags to attach to the event.</param>
        /// <returns><see langword="this"/> for convenient chaining.</returns>
        public static Activity? AddEvent(this Activity? activity, string eventName, IReadOnlyList<KeyValuePair<string, object?>>? tags = default)
        {
            if (activity == null) return activity;
            ActivityTagsCollection? tagsCollection = tags == null ? null : new ActivityTagsCollection(tags);
            return activity?.AddEvent(new ActivityEvent(eventName, tags: tagsCollection));
        }

        /// <summary>
        /// Add a new <see cref="ActivityLink"/> to the <see cref="Activity.Links"/> list.
        /// </summary>
        /// <param name="traceParent">The traceParent id for the associated <see cref="ActivityContext"/>.</param>
        /// <param name="tags">The optional list of tags to attach to the event.</param>
        /// <returns><see langword="this"/> for convenient chaining.</returns>
        public static Activity? AddLink(this Activity? activity, string traceParent, IReadOnlyList<KeyValuePair<string, object?>>? tags = default)
        {
            if (activity == null) return activity;
            ActivityTagsCollection? tagsCollection = tags == null ? null : new ActivityTagsCollection(tags);
            return activity?.AddLink(new ActivityLink(ActivityContext.Parse(traceParent, null), tags: tagsCollection));
        }

        /// <summary>
        /// Update the Activity to have a tag with an additional 'key' and value 'value'.
        /// This shows up in the <see cref="TagObjects"/> enumeration. It is meant for information that
        /// is useful to log but not needed for runtime control (for the latter, <see cref="Baggage"/>)
        /// </summary>
        /// <returns><see langword="this" /> for convenient chaining.</returns>
        /// <param name="key">The tag key name as a function</param>
        /// <param name="value">The tag value mapped to the input key as a function</param>
        public static Activity? AddTag(this Activity? activity, string key, Func<object?> value)
        {
            return activity?.AddTag(key, value());
        }

        /// <summary>
        /// Update the Activity to have a tag with an additional 'key' and value 'value'.
        /// This shows up in the <see cref="TagObjects"/> enumeration. It is meant for information that
        /// is useful to log but not needed for runtime control (for the latter, <see cref="Baggage"/>)
        /// </summary>
        /// <returns><see langword="this" /> for convenient chaining.</returns>
        /// <param name="key">The tag key name as a function</param>
        /// <param name="value">The tag value mapped to the input key</param>
        /// /// <param name="condition">The condition to check before adding the tag</param>
        public static Activity? AddConditionalTag(this Activity? activity, string key, string? value, Func<bool> condition)
        {
            if (condition())
            {
                return activity?.AddTag(key, value);
            }

            return activity;
        }

        /// <summary>
        /// Update the Activity to have a tag with an additional 'key' and value 'value'.
        /// This shows up in the <see cref="TagObjects"/> enumeration. It is meant for information that
        /// is useful to log but not needed for runtime control (for the latter, <see cref="Baggage"/>)
        /// </summary>
        /// <returns><see langword="this" /> for convenient chaining.</returns>
        /// <param name="key">The tag key name</param>
        /// <param name="value">The tag value mapped to the input key as a function</param>
        /// <param name="guidFormat">The format indicator for 16-byte GUID arrays.</param>
        public static Activity? AddTag(this Activity? activity, string key, byte[]? value, string? guidFormat)
        {
            if (value == null)
            {
                return activity?.AddTag(key, value);
            }
            if (value.Length == 16)
            {
                return activity?.AddTag(key, new Guid(value).ToString(guidFormat));
            }
            return activity?.AddTag(key, ToHexString(value));
        }

#if NET5_0_OR_GREATER
        private static string ToHexString(byte[] value) => Convert.ToHexString(value);
#else
        private static string ToHexString(byte[] value)
        {
            StringBuilder hex = new(value.Length * 2);
            foreach (byte b in value)
            {
                hex.AppendFormat("{0:x2}", b);
            }
            return hex.ToString();
        }
#endif
    }
}
