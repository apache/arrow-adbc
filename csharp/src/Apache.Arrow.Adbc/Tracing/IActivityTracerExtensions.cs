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
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Tracing
{
    public static class IActivityTracerExtensions
    {
        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="ActivityWithPii"/>.
        /// </summary>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="ActivityWithPii"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns>Returns a new <see cref="ActivityWithPii"/> object if there is any listener to the Activity, returns null otherwise</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="ActivityWithPii"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/>. If an exception is thrown by the delegate, the Activity
        /// status is set to <see cref="ActivityStatusCode.Error"/> and an Activity <see cref="ActivityEvent"/> is added to the activity
        /// and finally the exception is rethrown.
        /// </remarks>
        public static void TraceActivity(this IActivityTracer tracer, Action<ActivityWithPii?> call, [CallerMemberName] string? activityName = default, string? traceParent = default)
        {
            tracer.Trace.TraceActivity(call, activityName, traceParent ?? tracer.TraceParent);
        }

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="ActivityWithPii"/>.
        /// </summary>
        /// <typeparam name="T">The return type for the delegate.</typeparam>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="ActivityWithPii"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns>The result of the call to the delegate.</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="ActivityWithPii"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Event <see cref="ActivityEvent"/> is added to the activity and finally the exception is rethrown.
        /// </remarks>
        public static T TraceActivity<T>(this IActivityTracer tracer, Func<ActivityWithPii?, T> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            return tracer.Trace.TraceActivity(call, activityName, traceParent ?? tracer.TraceParent);
        }

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="ActivityWithPii"/>.
        /// </summary>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="ActivityWithPii"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns></returns>
        /// <remarks>
        /// Creates and starts a new <see cref="ActivityWithPii"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Event <see cref="ActivityEvent"/> is added to the activity and finally the exception is rethrown.
        /// </remarks>
        public static Task TraceActivityAsync(this IActivityTracer tracer, Func<ActivityWithPii?, Task> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            return tracer.Trace.TraceActivityAsync(call, activityName, traceParent ?? tracer.TraceParent);
        }

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="ActivityWithPii"/>.
        /// </summary>
        /// <typeparam name="T">The return type for the delegate.</typeparam>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="ActivityWithPii"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns>The result of the call to the delegate.</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="ActivityWithPii"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Event <see cref="ActivityEvent"/> is added to the activity and finally the exception is rethrown.
        /// </remarks>
        public static Task<T> TraceActivityAsync<T>(this IActivityTracer tracer, Func<ActivityWithPii?, Task<T>> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            return tracer.Trace.TraceActivityAsync(call, activityName, traceParent ?? tracer.TraceParent);
        }
    }
}
