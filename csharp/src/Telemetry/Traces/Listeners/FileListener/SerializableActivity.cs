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
using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Telemetry.Traces.Listeners.FileListener
{
    /// <summary>
    /// Simplified version of <see cref="Activity"/> that excludes some properties, etc.
    /// </summary>
    internal class SerializableActivity
    {
        [JsonConstructor]
        public SerializableActivity() { }

        internal SerializableActivity(
            ActivityStatusCode status,
            string? statusDescription,
            bool hasRemoteParent,
            ActivityKind kind,
            string operationName,
            TimeSpan duration,
            DateTime startTimeUtc,
            string? id,
            string? parentId,
            string? rootId,
            string? traceStateString,
            ActivitySpanId spanId,
            ActivityTraceId traceId,
            bool recorded,
            bool isAllDataRequested,
            ActivityTraceFlags activityTraceFlags,
            ActivitySpanId parentSpanId,
            ActivityIdFormat idFormat,
            IReadOnlyDictionary<string, object?> tagObjects,
            IReadOnlyList<SerializableActivityEvent> events,
            IReadOnlyList<SerializableActivityLink> links,
            IReadOnlyDictionary<string, string?> baggage)
        {
            Status = statusDescription ?? status.ToString();
            HasRemoteParent = hasRemoteParent;
            Kind = kind.ToString();
            OperationName = operationName;
            Duration = duration;
            StartTimeUtc = startTimeUtc;
            Id = id;
            ParentId = parentId;
            RootId = rootId;
            TraceStateString = traceStateString;
            SpanId = spanId.ToHexString();
            TraceId = traceId.ToHexString();
            Recorded = recorded;
            IsAllDataRequested = isAllDataRequested;
            ActivityTraceFlags = activityTraceFlags.ToString();
            ParentSpanId = parentSpanId.ToHexString();
            IdFormat = idFormat.ToString();
            TagObjects = tagObjects;
            Events = events;
            Links = links;
            Baggage = baggage;
        }

        internal SerializableActivity(Activity activity) : this(
            activity.Status,
            activity.StatusDescription,
            activity.HasRemoteParent,
            activity.Kind,
            activity.OperationName,
            activity.Duration,
            activity.StartTimeUtc,
            activity.Id,
            activity.ParentId,
            activity.RootId,
            activity.TraceStateString,
            activity.SpanId,
            activity.TraceId,
            activity.Recorded,
            activity.IsAllDataRequested,
            activity.ActivityTraceFlags,
            activity.ParentSpanId,
            activity.IdFormat,
            activity.TagObjects.ToDictionary(kv => kv.Key, kv => kv.Value),
            activity.Events.Select(e => (SerializableActivityEvent)e).ToArray(),
            activity.Links.Select(l => (SerializableActivityLink)l).ToArray(),
            activity.Baggage.ToDictionary(kv => kv.Key, kv => kv.Value))
        { }

        public string? Status { get; set; }
        public bool HasRemoteParent { get; set; }
        public string? Kind { get; set; }
        public string OperationName { get; set; } = "";
        public TimeSpan Duration { get; set; }
        public DateTime StartTimeUtc { get; set; }
        public string? Id { get; set; }
        public string? ParentId { get; set; }
        public string? RootId { get; set; }

        public string? TraceStateString { get; set; }
        public string? SpanId { get; set; }
        public string? TraceId { get; set; }
        public bool Recorded { get; set; }
        public bool IsAllDataRequested { get; set; }
        public string? ActivityTraceFlags { get; set; }
        public string? ParentSpanId { get; set; }
        public string? IdFormat { get; set; }

        public IReadOnlyDictionary<string, object?> TagObjects { get; set; } = new Dictionary<string, object?>();
        public IReadOnlyList<SerializableActivityEvent> Events { get; set; } = [];
        public IReadOnlyList<SerializableActivityLink> Links { get; set; } = [];
        public IReadOnlyDictionary<string, string?> Baggage { get; set; } = new Dictionary<string, string?>();
    }

    internal class SerializableActivityEvent
    {
        /// <summary>
        /// Gets the <see cref="SerializableActivityEvent"/> name.
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// Gets the <see cref="SerializableActivityEvent"/> timestamp.
        /// </summary>
        public DateTimeOffset Timestamp { get; set; }

        public IReadOnlyList<KeyValuePair<string, object?>> Tags { get; set; } = [];

        public static implicit operator SerializableActivityEvent(ActivityEvent source)
        {
            return new SerializableActivityEvent()
            {
                Name = source.Name,
                Timestamp = source.Timestamp,
                Tags = source.Tags.ToArray(),
            };
        }
    }

    internal class SerializableActivityLink
    {
        public SerializableActivityContext? Context { get; set; }

        public IReadOnlyList<KeyValuePair<string, object?>>? Tags { get; set; } = [];

        public static implicit operator SerializableActivityLink(ActivityLink source)
        {
            return new SerializableActivityLink()
            {
                Context = source.Context,
                Tags = source.Tags?.ToArray(),
            };
        }
    }

    internal class SerializableActivityContext
    {
        public string? SpanId { get; set; }
        public string? TraceId { get; set; }
        public string? TraceState { get; set; }
        public ActivityTraceFlags? TraceFlags { get; set; }
        public bool IsRemote { get; set; }

        public static implicit operator SerializableActivityContext(ActivityContext source)
        {
            return new SerializableActivityContext()
            {
                SpanId = source.SpanId.ToHexString(),
                TraceId = source.TraceId.ToHexString(),
                TraceState = source.TraceState,
                TraceFlags = source.TraceFlags,
                IsRemote = source.IsRemote,
            };
        }
    }
}
