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

using System.Collections.Generic;
using System.Diagnostics;

namespace Apache.Arrow.Adbc.Tracing
{
    public static class ActivityExtensions
    {
        /// <summary>
        /// Add a new <see cref="ActivityEvent"/> object to the <see cref="Activity.Events" /> list.
        /// </summary>
        /// <param name="activity">The activity to add the event to.</param>
        /// <param name="eventName">The name of the event.</param>
        /// <param name="tags">The optional list of tags to attach to the event.</param>
        /// <returns><see langword="this"/> for convenient chaining.</returns>
        public static Activity AddEvent(this Activity activity, string eventName, IReadOnlyList<KeyValuePair<string, object?>>? tags = default)
        {
            ActivityTagsCollection? tagsCollection = tags == null ? null : new ActivityTagsCollection(tags);
            return activity.AddEvent(new ActivityEvent(eventName, tags: tagsCollection));
        }

        /// <summary>
        /// Add a new <see cref="ActivityLink"/> to the <see cref="Activity.Links"/> list.
        /// </summary>
        /// <param name="activity">The activity to add the event to.</param>
        /// <param name="traceParent">The traceParent id for the associated <see cref="ActivityContext"/>.</param>
        /// <param name="tags">The optional list of tags to attach to the event.</param>
        /// <returns><see langword="this"/> for convenient chaining.</returns>
        public static Activity AddLink(this Activity activity, string traceParent, IReadOnlyList<KeyValuePair<string, object?>>? tags = default)
        {
            ActivityTagsCollection? tagsCollection = tags == null ? null : new ActivityTagsCollection(tags);
            return activity.AddLink(new ActivityLink(ActivityContext.Parse(traceParent, null), tags: tagsCollection));
        }
    }
}
