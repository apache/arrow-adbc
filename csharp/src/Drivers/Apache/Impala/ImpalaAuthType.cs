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

namespace Apache.Arrow.Adbc.Drivers.Apache.Impala
{
    internal enum ImpalaAuthType
    {
        Invalid = 0,
        None,
        UsernameOnly,
        Basic,
        Empty = int.MaxValue,
    }

    internal static class ImpalaAuthTypeParser
    {
        internal static bool TryParse(string? authType, out ImpalaAuthType authTypeValue)
        {
            switch (authType?.Trim().ToLowerInvariant())
            {
                case null:
                case "":
                    authTypeValue = ImpalaAuthType.Empty;
                    return true;
                case ImpalaAuthTypeConstants.None:
                    authTypeValue = ImpalaAuthType.None;
                    return true;
                case ImpalaAuthTypeConstants.UsernameOnly:
                    authTypeValue = ImpalaAuthType.UsernameOnly;
                    return true;
                case ImpalaAuthTypeConstants.Basic:
                    authTypeValue = ImpalaAuthType.Basic;
                    return true;
                default:
                    authTypeValue = ImpalaAuthType.Invalid;
                    return false;
            }
        }
    }
}
