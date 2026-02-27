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
using System.Reflection;
using System.Text;

namespace Apache.Arrow.Adbc.Tracing
{
    public class ActivityWithPii : IDisposable
    {
        private readonly Activity _activity;
        private readonly bool _shouldDisposeActivity;
        private bool _disposed;

        private ActivityWithPii(Activity activity, bool shouldDisposeActivity)
        {
            _activity = activity;
            _shouldDisposeActivity = shouldDisposeActivity;
        }

        /// <summary>
        /// Creates a new instance of <see cref="ActivityWithPii"/> from the given <see cref="Activity"/>.The new instance
        /// will call Dispose on the provided activity when this instance is disposed.
        ///<para>
        /// Use the <see cref="Wrap"/> method in the context of <see cref="Activity.Current"/>, where you don't own the <see cref="Activity"/> object and don't want
        /// to dispose it. Use the <see cref="New"/> method when starting a new activity via <see cref="ActivitySource.StartActivity"/>, which returns an <see cref="Activity"/> that
        /// should be disposed when the activity is stopped.
        /// </para>
        /// </summary>
        /// <param name="activity">The <see cref="Activity"/> to encapsulate.</param>
        /// <returns>A new instance of <see cref="ActivityWithPii"/></returns>
        public static ActivityWithPii? New(Activity? activity) => activity == null ? null : new ActivityWithPii(activity, shouldDisposeActivity: true);

        /// <summary>
        /// Creates a wrapper instance of <see cref="ActivityWithPii"/> for a given <see cref="Activity"/>. It will not dispose the
        /// provided activity.
        ///<para>
        /// Use the <see cref="Wrap"/> method in the context of <see cref="Activity.Current"/>, where you don't own the <see cref="Activity"/> object and don't want
        /// to dispose it. Use the <see cref="New"/> method when starting a new activity via <see cref="ActivitySource.StartActivity"/>, which returns an <see cref="Activity"/> that
        /// should be disposed when the activity is stopped.
        /// </para>
        /// </summary>
        /// <param name="activity">The <see cref="Activity"/> to wrap.</param>
        /// <returns>A new instance of <see cref="ActivityWithPii"/></returns>
        public static ActivityWithPii? Wrap(Activity? activity) => activity == null ? null : new ActivityWithPii(activity, shouldDisposeActivity: false);

        /// <summary>
        /// This is an ID that is specific to a particular request.   Filtering
        /// to a particular ID insures that you get only one request that matches.
        /// Id has a hierarchical structure: '|root-id.id1_id2.id3_' Id is generated when
        /// <see cref="Start"/> is called by appending suffix to Parent.Id
        /// or ParentId; Activity has no Id until it started
        /// <para/>
        /// See <see href="https://github.com/dotnet/runtime/blob/main/src/libraries/System.Diagnostics.DiagnosticSource/src/ActivityUserGuide.md#id-format"/> for more details
        /// </summary>
        /// <example>
        /// Id looks like '|a000b421-5d183ab6.1.8e2d4c28_1.':<para />
        ///  - '|a000b421-5d183ab6.' - Id of the first, top-most, Activity created<para />
        ///  - '|a000b421-5d183ab6.1.' - Id of a child activity. It was started in the same process as the first activity and ends with '.'<para />
        ///  - '|a000b421-5d183ab6.1.8e2d4c28_' - Id of the grand child activity. It was started in another process and ends with '_'<para />
        /// 'a000b421-5d183ab6' is a <see cref="RootId"/> for the first Activity and all its children
        /// </example>
        public string? Id => _activity.Id;

        /// <summary>
        /// Gets status code of the current activity object.
        /// </summary>
        public ActivityStatusCode Status => _activity.Status;

        /// <summary>
        /// Holds the W3C 'tracestate' header as a string.
        ///
        /// Tracestate is intended to carry information supplemental to trace identity contained
        /// in traceparent. List of key value pairs carried by tracestate convey information
        /// about request position in multiple distributed tracing graphs. It is typically used
        /// by distributed tracing systems and should not be used as a general purpose baggage
        /// as this use may break correlation of a distributed trace.
        ///
        /// Logically it is just a kind of baggage (if flows just like baggage), but because
        /// it is expected to be special cased (it has its own HTTP header), it is more
        /// convenient/efficient if it is not lumped in with other baggage.
        /// </summary>
        public string? TraceStateString
        {
            get => _activity.TraceStateString;
            set => _activity.TraceStateString = value;
        }

        /// <summary>
        /// Update the Activity to have baggage with an additional 'key' and value 'value'.
        /// This shows up in the <see cref="Baggage"/> enumeration as well as the <see cref="GetBaggageItem(string)"/>
        /// method.
        /// Baggage is meant for information that is needed for runtime control.   For information
        /// that is simply useful to show up in the log with the activity use <see cref="Tags"/>.
        /// Returns 'this' for convenient chaining.
        /// </summary>
        /// <returns><see langword="this" /> for convenient chaining.</returns>
        public ActivityWithPii AddBaggage(string key, string? value, bool isPii = true)
        {
            bool shouldRedact = isPii;
            _activity.AddBaggage(key, shouldRedact ? RedactedValue.DefaultValue : value);
            return this;
        }

        public ActivityWithPii AddEvent(ActivityEvent e, bool isPii = true)
        {
            ActivityEvent clone = new(e.Name, e.Timestamp, [.. isPii ? CloneTagsWithRedaction(e.Tags) : e.Tags]);
            _activity.AddEvent(clone);
            return this;
        }

        /// <summary>
        /// Add a new <see cref="ActivityEvent"/> object to the <see cref="Activity.Events" /> list.
        /// </summary>
        /// <param name="eventName">The name of the event.</param>
        /// <param name="tags">The optional list of tags to attach to the event.</param>
        /// <returns><see langword="this"/> for convenient chaining.</returns>
        public ActivityWithPii AddEvent(string eventName, IReadOnlyList<KeyValuePair<string, object?>>? tags = default, bool isPii = true)
        {
            ActivityTagsCollection? tagsCollection = tags == null ? null : [.. tags];
            return AddEvent(new ActivityEvent(eventName, tags: tagsCollection), isPii);
        }

        public ActivityWithPii AddException(Exception exception, in TagList tags = default, DateTimeOffset timestamp = default, bool exceptionHasPii = true)
        {
            if (exception == null)
            {
                throw new ArgumentNullException(nameof(exception));
            }

            if (!exceptionHasPii)
            {
                _activity.AddException(exception, tags, timestamp);
                return this;
            }

            TagList exceptionTags = tags;
            const string ExceptionMessageTag = "exception.message";
            bool hasMessage = false;

            for (int i = 0; i < exceptionTags.Count; i++)
            {
                if (exceptionTags[i].Key == ExceptionMessageTag)
                {
                    exceptionTags[i] = new KeyValuePair<string, object?>(
                        exceptionTags[i].Key,
                        exceptionTags[i].Value is RedactedValue ? exceptionTags[i].Value : new RedactedValue(exceptionTags[i].Value));
                    hasMessage = true;
                }

                // TODO: Decide how to handle other unknown tags that may contain PII.
                // For now, we only handle the well-known "exception.message" tag, but there may be other tags that also contain PII and should be redacted.
            }

            if (!hasMessage)
            {
                exceptionTags.Add(new KeyValuePair<string, object?>(ExceptionMessageTag, new RedactedValue(exception.Message)));
            }

            _activity.AddException(exception, exceptionTags, timestamp);
            return this;
        }

        /// <summary>
        /// Add an <see cref="ActivityLink"/> to the <see cref="Links"/> list.
        /// </summary>
        /// <param name="link">The <see cref="ActivityLink"/> to add.</param>
        /// <returns><see langword="this" /> for convenient chaining.</returns>
        /// <remarks>
        /// For contexts that are available during span creation, adding links at span creation is preferred to calling <see cref="AddLink(ActivityLink)" /> later,
        /// because head sampling decisions can only consider information present during span creation.
        /// </remarks>
        public ActivityWithPii AddLink(ActivityLink link, bool isPii = true)
        {
            if (link.Tags == null)
            {
                _activity.AddLink(link);
                return this;
            }

            ActivityLink clone = new(link.Context, [.. isPii ? CloneTagsWithRedaction(link.Tags) : link.Tags]);
            _activity.AddLink(clone);
            return this;
        }

        /// <summary>
        /// Add an <see cref="ActivityLink"/> to the <see cref="Links"/> list.
        /// </summary>
        /// <param name="traceParent"></param>
        /// <param name="tags"></param>
        /// <param name="isPii"></param>
        /// <returns></returns>
        public ActivityWithPii AddLink(string traceParent, IReadOnlyList<KeyValuePair<string, object?>>? tags = default, bool isPii = true)
        {
            ActivityTagsCollection? tagsCollection = tags == null ? null : [.. tags];
            return AddLink(new ActivityLink(ActivityContext.Parse(traceParent, null), tags: tagsCollection), isPii);
        }

        /// <summary>
        /// Update the Activity to have a tag with an additional 'key' and value 'value'.
        /// This shows up in the <see cref="Activity.TagObjects"/> enumeration. It is meant for information that
        /// is useful to log but not needed for runtime control (for the latter, <see cref="Activity.Baggage"/>)
        /// <para>
        /// Note: Treating string tags the same as any other object.
        /// </para>
        /// </summary>
        /// <returns><see langword="this" /> for convenient chaining.</returns>
        /// <param name="key">The tag key name</param>
        /// <param name="value">The tag value mapped to the input key</param>
        public ActivityWithPii AddTag(string key, object? value, bool isPii = true)
        {
            object? finalValue = value;
            switch (value)
            {
                case Delegate:
                    MethodInfo methodInfo = value.GetType().GetMethod("Invoke")!;
                    if (methodInfo.GetParameters().Length != 0)
                    {
                        throw new NotSupportedException("Only parameterless delegates are supported as tag values");
                    }
                    object? returnValue = methodInfo.Invoke(value, []);
                    return AddTag(key, returnValue, isPii);
            }

            bool shouldRedact = (isPii && finalValue is not RedactedValue);
            _activity.AddTag(key, shouldRedact ? new RedactedValue(finalValue) : finalValue);
            return this;
        }

        /// <summary>
        /// Add or update the Activity tag with the input key and value.
        /// If the input value is null
        ///     - if the collection has any tag with the same key, then this tag will get removed from the collection.
        ///     - otherwise, nothing will happen and the collection will not change.
        /// If the input value is not null
        ///     - if the collection has any tag with the same key, then the value mapped to this key will get updated with the new input value.
        ///     - otherwise, the key and value will get added as a new tag to the collection.
        /// </summary>
        /// <param name="key">The tag key name</param>
        /// <param name="value">The tag value mapped to the input key</param>
        /// <returns><see langword="this" /> for convenient chaining.</returns>
        public ActivityWithPii SetTag(string key, object? value, bool isPii = true)
        {
            bool shouldRedact = (isPii && value is not RedactedValue);
            _activity.SetTag(key, shouldRedact ? new RedactedValue(value) : value);
            return this;
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
        public void SetStatus(ActivityStatusCode statusCode, string? description = null)
        {
            _activity.SetStatus(statusCode, description);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    if (_shouldDisposeActivity)
                    {
                        _activity.Dispose();
                    }
                }

                _disposed = true;
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        private static IEnumerable<KeyValuePair<string, object?>> CloneTagsWithRedaction(IEnumerable<KeyValuePair<string, object?>> tags)
        {
            foreach (KeyValuePair<string, object?> tag in tags)
            {
                yield return new KeyValuePair<string, object?>(tag.Key, tag.Value is RedactedValue ? tag.Value : new RedactedValue(tag.Value));
            }
        }

#if NET5_0_OR_GREATER
        public static string ToHexString(byte[] value) => Convert.ToHexString(value);
#else
        public static string ToHexString(byte[] value)
        {
            if (value.Length == 16)
            {
                return new Guid(value).ToString("N");
            }
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
