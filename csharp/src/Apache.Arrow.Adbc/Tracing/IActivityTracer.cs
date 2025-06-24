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
    /// Provides a base implementation for a tracing source. If drivers want to enable tracing,
    /// they need to add a trace listener (e.g., <see cref="FileExporter"/>).
    /// </summary>
    public interface IActivityTracer
    {
        /// <summary>
        /// Gets the <see cref="ActivityTrace"/>
        /// </summary>
        ActivityTrace Trace { get; }

        /// <summary>
        /// Gets the value of the trace parent.
        /// </summary>
        string? TraceParent { get; }

        /// <summary>
        /// Gets the product version from the file version of the current assembly.
        /// </summary>
        /// <returns></returns>
        string AssemblyVersion { get; }

        /// <summary>
        /// Gets the (simple) assembly name for the current (virtual) object.
        /// </summary>
        /// <returns></returns>
        string AssemblyName { get; }
    }
}
