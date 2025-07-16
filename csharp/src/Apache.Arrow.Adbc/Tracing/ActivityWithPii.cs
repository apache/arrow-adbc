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
using System.Linq;
using System.Text;

namespace Apache.Arrow.Adbc.Tracing
{
    /// <summary>
    /// ActivityWithPii is a wrapper to the <see cref="Activity "/> which represents operation with
    /// context to be used for logging.
    ///
    /// By default, all values added the ActivityWithPii are considered to be PII (Personally Identifiable Information).
    ///
    /// To indicate that a value is not PII, you can use the `isPii` parameter in methods.
    /// </summary>
    public sealed class ActivityWithPii : IDisposable
    {
        private readonly Activity _activity;

        internal ActivityWithPii(Activity activity)
        {
            _activity = activity;
        }

        /// <summary>
        /// Update the Activity to have baggage with an additional 'key' and value 'value'. Use parameter 'isPii' to indicate
        /// whether the value is PII (Personally Identifiable Information).
        /// This shows up in the <see cref="Baggage"/> enumeration as well as the <see cref="GetBaggageItem(string)"/>
        /// method.
        /// Baggage is meant for information that is needed for runtime control.   For information
        /// that is simply useful to show up in the log with the activity use <see cref="Tags"/>.
        /// Returns 'this' for convenient chaining.
        /// </summary>
        /// <returns><see langword="this" /> for convenient chaining.</returns>
        public ActivityWithPii AddBaggage(string key, string? value, bool isPii = true)
        {
            object? obj = MaybeToRedactedValue(value, isPii);
            _activity.AddBaggage(key, obj?.ToString());
            return this;
        }

        /// <summary>
        /// Add <see cref="ActivityEvent" /> object to the <see cref="Events" /> list. Use parameter 'isPii' to indicate
        /// whether the tags contain PII (Personally Identifiable Information).
        /// </summary>
        /// <param name="e"> object of <see cref="ActivityEvent"/> to add to the attached events list.</param>
        /// <returns><see langword="this" /> for convenient chaining.</returns>
        public ActivityWithPii AddEvent(string eventName, IReadOnlyList<KeyValuePair<string, object?>>? tags = default, bool isPii = true)
        {
            ActivityTagsCollection? tagsCollection = ToActivityTagsCollection(tags, isPii);
            _activity.AddEvent(new ActivityEvent(eventName, tags: tagsCollection));
            return this;
        }

        /// <summary>
        /// Add an <see cref="ActivityEvent" /> object containing the exception information to the <see cref="Events" /> list.
        /// </summary>
        /// <param name="exception">The exception to add to the attached events list.</param>
        /// <param name="isPii">Indicates whether the exception message contain PII (Personally Identifiable Information).</param>
        /// <returns><see langword="this" /> for convenient chaining.</returns>
        /// <remarks>
        /// <para>- The name of the event will be "exception", and it will include the tags "exception.message", "exception.stacktrace", and "exception.type".</para>
        /// <para>- Any registered <see cref="ActivityListener"/> with the <see cref="ActivityListener.ExceptionRecorder"/> callback will be notified about this exception addition
        /// before the <see cref="ActivityEvent" /> object is added to the <see cref="Events" /> list.</para>
        /// <para>- Any registered <see cref="ActivityListener"/> with the <see cref="ActivityListener.ExceptionRecorder"/> callback that adds "exception.message", "exception.stacktrace", or "exception.type" tags
        /// will not have these tags overwritten, except by any subsequent <see cref="ActivityListener"/> that explicitly overwrites them.</para>
        /// </remarks>
        public ActivityWithPii AddException(Exception exception, bool isPii = true)
        {
            const string ExceptionMessageTag = "exception.message";
            TagList tagList = new();
            if (isPii)
            {
                tagList.Add(new KeyValuePair<string, object?>(ExceptionMessageTag, new RedactedValue(exception.Message)));
            }

            _activity.AddException(exception, tagList);
            return this;
        }

        /// <summary>
        /// Add a new <see cref="ActivityLink"/> to the <see cref="Activity.Links"/> list.
        /// </summary>
        /// <param name="traceParent">The traceParent id for the associated <see cref="ActivityContext"/>.</param>
        /// <param name="tags">The optional list of tags to attach to the event.</param>
        /// <param name="isPii">Indicates whether the tags contain PII (Personally Identifiable Information).</param>
        /// <returns><see langword="this"/> for convenient chaining.</returns>
        public ActivityWithPii AddLink(string traceParent, IReadOnlyList<KeyValuePair<string, object?>>? tags = default, bool isPii = true)
        {
            ActivityTagsCollection? tagsCollection = ToActivityTagsCollection(tags, isPii);
            _activity?.AddLink(new ActivityLink(ActivityContext.Parse(traceParent, null), tags: tagsCollection));
            return this;
        }

        /// <summary>
        /// Update the Activity to have a tag with an additional 'key' and value 'value'.
        /// This shows up in the <see cref="TagObjects"/> enumeration. It is meant for information that
        /// is useful to log but not needed for runtime control (for the latter, <see cref="Baggage"/>)
        /// </summary>
        /// <returns><see langword="this" /> for convenient chaining.</returns>
        /// <param name="key">The tag key name</param>
        /// <param name="value">The tag value mapped to the input key</param>
        /// <param name="isPii">Indicates whether the value is PII (Personally Identifiable Information).</param>
        public ActivityWithPii AddTag(string key, object? value, bool isPii = true)
        {
            value = MaybeToRedactedValue(value, isPii);
            _activity.AddTag(key, value);
            return this;
        }

        /// <summary>
        /// Update the Activity to have a tag with an additional 'key' and value 'value'.
        /// This shows up in the <see cref="TagObjects"/> enumeration. It is meant for information that
        /// is useful to log but not needed for runtime control (for the latter, <see cref="Baggage"/>)
        /// </summary>
        /// <returns><see langword="this" /> for convenient chaining.</returns>
        /// <param name="key">The tag key name</param>
        /// <param name="valueFn">The tag value function mapped to the input key</param>
        /// <param name="isPii">Indicates whether the value is PII (Personally Identifiable Information).</param>
        public ActivityWithPii AddTag(string key, Func<object?> valueFn, bool isPii = true)
        {
            object? value = MaybeToRedactedValue(valueFn(), isPii);
            _activity.AddTag(key, value);
            return this;
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
        public ActivityWithPii AddTag(string key, byte[]? value, string? guidFormat, bool isPii = true)
        {
            if (value == null)
            {
                return AddTag(key, value, isPii);
            }
            if (value.Length == 16)
            {
                return AddTag(key, new Guid(value).ToString(guidFormat), isPii);
            }
            return AddTag(key, ToHexString(value), isPii);
        }

        /// <summary>
        /// Sets the status code and description on the current activity object.
        /// </summary>
        /// <param name="code">The status code</param>
        /// <param name="description">The error status description</param>
        /// <returns><see langword="this" /> for convenient chaining.</returns>
        /// <remarks>
        /// When passing code value different than ActivityStatusCode.Error, the Activity.StatusDescription will reset to null value.
        /// The description parameter will be respected only when passing ActivityStatusCode.Error value.
        /// </remarks>
        public ActivityWithPii SetStatus(ActivityStatusCode code, string? description = null)
        {
            _activity.SetStatus(code, description);
            return this;
        }

        /// <summary>
        /// Gets status code of the current activity object.
        /// </summary>
        public ActivityStatusCode Status => _activity.Status;

        /// <summary>
        /// Dipsoses the current <see cref="Activity"/> object.
        /// </summary>
        public void Dispose()
        {
            _activity.Dispose();
        }

        private static object? MaybeToRedactedValue(object? value, bool isPii) =>
            (value is RedactedValue redactedValue)
                ? redactedValue
                : (isPii && value != null)
                    ? new RedactedValue(value, isPii)
                    : value;

        private static ActivityTagsCollection? ToActivityTagsCollection(IReadOnlyList<KeyValuePair<string, object?>>? tags, bool isPii)
        {
            IEnumerable<KeyValuePair<string, object?>>? tagsCopy = isPii
                ? (tags?.Select(x => new KeyValuePair<string, object?>(x.Key, MaybeToRedactedValue(x.Value, isPii))))
                : (tags?.ToList());
            return tagsCopy == null ? null : [.. tagsCopy];
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
