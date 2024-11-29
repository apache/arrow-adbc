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

namespace Apache.Arrow.Adbc.Tracing
{
    /// <summary>
    /// Simplified version of <see cref="Activity"/> that exclude <see cref="Activity.Parent"/>, etc.
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
            string displayName,
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
            IReadOnlyList<KeyValuePair<string, object?>> tagObjects,
            IReadOnlyList<ActivityEvent> events,
            IReadOnlyList<ActivityLink> links,
            IReadOnlyList<KeyValuePair<string, string?>> baggage)
        {
            Status = status;
            StatusDescription = statusDescription ?? status.ToString();
            HasRemoteParent = hasRemoteParent;
            Kind = kind;
            OperationName = operationName;
            DisplayName = displayName;
            Duration = duration;
            StartTimeUtc = startTimeUtc;
            Id = id;
            ParentId = parentId;
            RootId = rootId;
            TraceStateString = traceStateString;
            SpanId = spanId;
            TraceId = traceId;
            Recorded = recorded;
            IsAllDataRequested = isAllDataRequested;
            ActivityTraceFlags = activityTraceFlags;
            ParentSpanId = parentSpanId;
            IdFormat = idFormat;
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
            activity.DisplayName,
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
            activity.TagObjects.ToArray(),
            activity.Events.ToArray(),
            activity.Links.ToArray(),
            activity.Baggage.ToArray())
        { }

        public ActivityStatusCode Status { get; set; }
        public string? StatusDescription { get; set; }
        public bool HasRemoteParent { get; set; }
        public ActivityKind Kind { get; set; }
        public string OperationName { get; set; } = "";
        public string DisplayName { get; set; } = "";
        public TimeSpan Duration { get; set; }
        public DateTime StartTimeUtc { get; set; }
        public string? Id { get; set; }
        public string? ParentId { get; set; }
        public string? RootId { get; set; }

        public string? TraceStateString { get; set; }
        public ActivitySpanId SpanId { get; set; }
        public ActivityTraceId TraceId { get; set; }
        public bool Recorded { get; set; }
        public bool IsAllDataRequested { get; set; }
        public ActivityTraceFlags ActivityTraceFlags { get; set; }
        public ActivitySpanId ParentSpanId { get; set; }
        public ActivityIdFormat IdFormat { get; set; }

        public IReadOnlyList<KeyValuePair<string, object?>> TagObjects { get; set; } = [];
        public IReadOnlyList<ActivityEvent> Events { get; set; } = [];
        public IReadOnlyList<ActivityLink> Links { get; set; } = [];
        public IReadOnlyList<KeyValuePair<string, string?>> Baggage { get; set; } = [];
    }
}
