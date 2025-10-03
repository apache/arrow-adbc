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
using Google;

namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    internal class BigQueryUtils
    {
        public static bool TokenRequiresUpdate(Exception ex)
        {
            bool result = false;

            if (ex is GoogleApiException gaex && gaex.HttpStatusCode == System.Net.HttpStatusCode.Unauthorized)
            {
                result = true;
            }

            return result;
        }

        internal static string BigQueryAssemblyName = GetAssemblyName(typeof(BigQueryConnection));

        internal static string BigQueryAssemblyVersion = GetAssemblyVersion(typeof(BigQueryConnection));

        internal static string GetAssemblyName(Type type) => type.Assembly.GetName().Name!;

        internal static string GetAssemblyVersion(Type type) => FileVersionInfo.GetVersionInfo(type.Assembly.Location).ProductVersion ?? string.Empty;

        public static bool ContainsException<T>(Exception exception, out T? containedException) where T : Exception
        {
            if (exception is AggregateException aggregateException)
            {
                foreach (Exception? ex in aggregateException.InnerExceptions)
                {
                    if (ContainsException(ex, out T? inner))
                    {
                        containedException = inner;
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
    }
}
