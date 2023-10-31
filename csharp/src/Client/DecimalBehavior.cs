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
using System.Data.SqlTypes;

namespace Apache.Arrow.Adbc.Client
{
    /// <summary>
    /// Some callers may choose to leverage the default
    /// <see cref="SqlDecimal"/> values to get a full range of precision
    /// and scale support, while others may opt to restrict
    /// use to .NET's <see cref="decimal"/> range
    /// </summary>
    public enum DecimalBehavior
    {
        /// <summary>
        /// Use <see cref="SqlDecimal"/>
        /// </summary>
        UseSqlDecimal,

        /// <summary>
        /// Use <see cref="decimal"/>
        /// and treat <see cref="OverflowException"/>
        /// as string values
        /// </summary>
        OverflowDecimalAsString
    }
}
