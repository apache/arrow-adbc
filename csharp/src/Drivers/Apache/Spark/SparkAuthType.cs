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

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal enum SparkAuthType
    {
        None,
        UsernameOnly,
        Basic,
        Token,
        Empty = int.MaxValue,
    }

    internal static class SparkAuthTypeParser
    {
        internal static bool TryParse(string? authType, out SparkAuthType authTypeValue)
        {
            switch (authType?.Trim().ToLowerInvariant())
            {
                case null:
                case "":
                    authTypeValue = SparkAuthType.Empty;
                    return true;
                case SparkAuthTypeConstants.None:
                    authTypeValue = SparkAuthType.None;
                    return true;
                case SparkAuthTypeConstants.UsernameOnly:
                    authTypeValue = SparkAuthType.UsernameOnly;
                    return true;
                case SparkAuthTypeConstants.Basic:
                    authTypeValue = SparkAuthType.Basic;
                    return true;
                case SparkAuthTypeConstants.Token:
                    authTypeValue = SparkAuthType.Token;
                    return true;
                default:
                    authTypeValue = default;
                    return false;
            }
        }
    }
}
