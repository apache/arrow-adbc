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
using System.Threading;
using System.Text.RegularExpressions;
using Thrift.Transport;

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

            if (timeout == 0 || timeout == int.MaxValue)
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
            if (!string.IsNullOrEmpty(value) && int.TryParse(value, out int queryTimeout) && (queryTimeout >= 0))
            {
                queryTimeoutSeconds = queryTimeout;
                return true;
            }
            else
            {
                throw new ArgumentOutOfRangeException(key, value, $"The value '{value}' for option '{key}' is invalid. Must be a numeric value of 0 (infinite) or greater.");
            }
        }

        public static bool BooleanIsValid(string key, string value, out bool booleanValue)
        {
            if (bool.TryParse(value, out booleanValue))
            {
                return true;
            }
            else
            {
                throw new ArgumentOutOfRangeException(key, nameof(value), $"Invalid value for {key}: {value}. Expected a boolean value.");
            }
        }

        public static bool ContainsException<T>(Exception exception, out T? containedException) where T : Exception
        {
            if (exception is AggregateException aggregateException)
            {
                foreach (Exception? ex in aggregateException.InnerExceptions)
                {
                    if (ex is T ce)
                    {
                        containedException = ce;
                        return true;
                    }
                }
            }

            Exception? e = exception;
            while (e != null)
            {
                if (e is T ce)
                {
                    containedException = ce;
                    return true;
                }
                e = e.InnerException;
            }

            containedException = null;
            return false;
        }

        public static bool ContainsException(Exception exception, Type? exceptionType, out Exception? containedException)
        {
            if (exception == null || exceptionType == null)
            {
                containedException = null;
                return false;
            }

            if (exception is AggregateException aggregateException)
            {
                foreach (Exception? ex in aggregateException.InnerExceptions)
                {
                    if (exceptionType.IsInstanceOfType(ex))
                    {
                        containedException = ex;
                        return true;
                    }
                }
            }

            Exception? e = exception;
            while (e != null)
            {
                if (exceptionType.IsInstanceOfType(e))
                {
                    containedException = e;
                    return true;
                }
                e = e.InnerException;
            }

            containedException = null;
            return false;
        }

        internal static string FormatExceptionMessage(Exception exception)
        {
            if (exception is AggregateException aEx)
            {
                AggregateException flattenedEx = aEx.Flatten();
                IEnumerable<string> messages = flattenedEx.InnerExceptions
                    .Select((ex, index) => $"({index + 1}) {FormatTransportExceptionMessage(ex)}");
                string fullMessage = $"{FormatTransportExceptionMessage(flattenedEx)}: {string.Join(", ", messages)}";
                return fullMessage;
            }

            return FormatTransportExceptionMessage(exception);
        }

        /// <summary>
        /// Formats a transport exception message with custom handling for HTTP status codes (401, 403, 404).
        /// </summary>
        internal static string FormatTransportExceptionMessage(Exception exception)
        {
            var customMsg = "";
            // Use ContainsException to robustly find TTransportException or HttpRequestException
            if (exception is TTransportException transportEx)
            {
                 // Try to extract status code from the message
                var match = System.Text.RegularExpressions.Regex.Match(transportEx.Message, @"\b(\d{3})\b");
                if (match.Success && int.TryParse(match.Value, out int code))
                {
                    switch (code)
                    {
                        case 401:
                            customMsg = "Unauthorized (401): Please check your credentials.";
                            break;
                        case 403:
                            customMsg = "Forbidden (403): You do not have permission to access this resource.";
                            break;
                        case 404:
                            customMsg = "Not Found (404): The requested resource was not found on the server.";
                            break;
                        default:
                            break;
                    }
                }
            }
            return $"{customMsg}{Environment.NewLine}{exception.Message}";
        }
    }
}
