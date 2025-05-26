﻿/*
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
    /// <summary>
    /// Provides a base implementation for a tracing source. If drivers want to enable tracing,
    /// they need to add a trace listener (e.g., <see cref="FileExporter"/>).
    /// </summary>
    public interface IActivityTracer : IDisposable
    {
        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns>Returns a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/>. If an exception is thrown by the delegate, the Activity
        /// status is set to <see cref="ActivityStatusCode.Error"/> and an Activity <see cref="ActivityEvent"/> is added to the actitity
        /// and finally the exception is rethrown.
        /// </remarks>
        public void TraceActivity(Action<Activity?> call, [CallerMemberName] string? activityName = default, string? traceParent = default);

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <typeparam name="T">The return type for the delegate.</typeparam>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns>The result of the call to the delegate.</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Event <see cref="ActivityEvent"/> is added to the actitity and finally the exception is rethrown.
        /// </remarks>
        public T TraceActivity<T>(Func<Activity?, T> call, [CallerMemberName] string? activityName = default, string? traceParent = default);

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns></returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Event <see cref="ActivityEvent"/> is added to the actitity and finally the exception is rethrown.
        /// </remarks>
        public Task TraceActivityAsync(Func<Activity?, Task> call, [CallerMemberName] string? activityName = default, string? traceParent = default);

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <typeparam name="T">The return type for the delegate.</typeparam>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns>The result of the call to the delegate.</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Event <see cref="ActivityEvent"/> is added to the actitity and finally the exception is rethrown.
        /// </remarks>
        public Task<T> TraceActivityAsync<T>(Func<Activity?, Task<T>> call, [CallerMemberName] string? activityName = default, string? traceParent = default);
    }
}
