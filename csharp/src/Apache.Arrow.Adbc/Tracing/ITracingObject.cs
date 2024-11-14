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

namespace Apache.Arrow.Adbc.Tracing
{
    /// <summary>
    /// Provides interface for a tracing object.
    /// </summary>
    public interface ITracingObject
    {
        /// <summary>
        /// Gets the <see cref="System.Diagnostics.ActivitySource"/> for the current object. The value may be null if the containing object
        /// did not set this property - typically because tracing was not enabled.
        /// </summary>
        ActivitySource? ActivitySource { get; }

        /// <summary>
        /// Starts a new activity with a given name.
        /// </summary>
        /// <param name="methodName"></param>
        /// <returns>Returns a new <see cref="System.Diagnostics.Activity"/> instance if there is an active listener for the <see cref="ActivitySource"/>.
        /// It returns <c>null</c>, otherwise.</returns>
        Activity? StartActivity(string methodName);

        /// <summary>
        /// Gets the base name of the tracing object. Typically this is the full name of the class that is tracing.
        /// </summary>
        string TracingBaseName { get; }

        void TraceException(Exception exception, Activity? activity, bool escaped = true);
    }
}
