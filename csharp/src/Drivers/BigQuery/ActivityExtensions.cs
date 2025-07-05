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
using System.Text;
using Apache.Arrow.Adbc.Tracing;

namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    internal static class ActivityExtensions
    {
        private const string bigQueryKeyPrefix = "adbc.bigquery.tracing.";
        private const string bigQueryParameterKeyValueSuffix = ".value";

        public static Activity AddBigQueryTag(this Activity activity, string key, object? value)
        {
            string bigQueryKey = bigQueryKeyPrefix + key;
            return activity.AddTag(bigQueryKey, value);
        }

        public static Activity AddConditionalBigQueryTag(this Activity activity, string key, string? value, bool condition)
        {
            string bigQueryKey = bigQueryKeyPrefix + key;
            return activity.AddConditionalTag(key, value, condition)!;
        }

        public static Activity AddBigQueryParameterTag(this Activity activity, string parameterName, object? value)
        {
            if (BigQueryParameters.IsSafeToLog(parameterName))
            {
                string bigQueryParameterValueKey = parameterName + bigQueryParameterKeyValueSuffix;
                return activity.AddTag(bigQueryParameterValueKey, value);
            }

            return activity;
        }
    }
}
