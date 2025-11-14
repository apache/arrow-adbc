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

namespace Apache.Arrow.Adbc.Drivers.Databricks.Auth
{
    /// <summary>
    /// Enum representing the OAuth grant types supported by the Databricks driver.
    /// </summary>
    internal enum DatabricksOAuthGrantType
    {
        /// <summary>
        /// Use a pre-generated Databricks personal access token for authentication. Default value.
        /// </summary>
        AccessToken,

        /// <summary>
        /// Use OAuth client credentials flow for m2m authentication.
        /// </summary>
        ClientCredentials,
    }

    /// <summary>
    /// Parser for converting string grant type values to the DatabricksOAuthGrantType enum.
    /// </summary>
    internal static class DatabricksOAuthGrantTypeParser
    {
        /// <summary>
        /// Tries to parse a string grant type value to the corresponding DatabricksOAuthGrantType enum value. If
        /// the grant type is not supported, the default value is returned.
        /// </summary>
        /// <param name="grantType">The string grant type value to parse.</param>
        /// <param name="grantTypeValue">The parsed DatabricksOAuthGrantType enum value.</param>
        /// <returns>True if the parsing was successful, false otherwise.</returns>
        internal static bool TryParse(string? grantType, out DatabricksOAuthGrantType grantTypeValue)
        {
            switch (grantType?.Trim().ToLowerInvariant())
            {
                case null:
                case "":
                    grantTypeValue = DatabricksOAuthGrantType.AccessToken;
                    return true;
                case DatabricksConstants.OAuthGrantTypes.AccessToken:
                    grantTypeValue = DatabricksOAuthGrantType.AccessToken;
                    return true;
                case DatabricksConstants.OAuthGrantTypes.ClientCredentials:
                    grantTypeValue = DatabricksOAuthGrantType.ClientCredentials;
                    return true;
                default:
                    grantTypeValue = DatabricksOAuthGrantType.AccessToken;
                    return false;
            }
        }
    }
}
