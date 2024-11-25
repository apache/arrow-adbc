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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    internal class ApacheUtility
    {
        internal const int QueryTimeoutSecondsDefault = 60;

        public enum TimeUnit
        {
            Seconds,
            Milliseconds
        }

        public static CancellationToken GetCancellationToken(int timeout, TimeUnit timeUnit)
        {
            TimeSpan span;

            if (timeout == -1 || timeout == int.MaxValue)
            {
                // the max TimeSpan for CancellationTokenSource is int.MaxValue in milliseconds (not TimeSpan.MaxValue)
                // no matter what the unit is
                span = TimeSpan.FromMilliseconds(int.MaxValue);
            }
            else
            {
                if (timeUnit == TimeUnit.Seconds)
                {
                    span = TimeSpan.FromSeconds(timeout);
                }
                else
                {
                    span = TimeSpan.FromMilliseconds(timeout);
                }
            }

            return GetCancellationToken(span);
        }

        private static CancellationToken GetCancellationToken(TimeSpan timeSpan)
        {
            var cts = new CancellationTokenSource(timeSpan);
            return cts.Token;
        }

        public static bool QueryTimeoutIsValid(string key, string value, out int queryTimeoutSeconds)
        {
            if (!string.IsNullOrEmpty(value) && int.TryParse(value, out int queryTimeout) && (queryTimeout > 0 || queryTimeout == -1))
            {
                queryTimeoutSeconds = queryTimeout;
                return true;
            }
            else
            {
                throw new ArgumentOutOfRangeException(key, value, $"The value '{value}' for option '{key}' is invalid. Must be a numeric value of -1 (infinite) or greater than zero.");
            }
        }
    }
}
