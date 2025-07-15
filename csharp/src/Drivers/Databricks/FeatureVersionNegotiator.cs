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

using Apache.Hive.Service.Rpc.Thrift;
using System;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Negotiates protocol features between client and server based on protocol version.
    /// This class helps determine which features are available based on the negotiated protocol version.
    /// </summary>
    internal static class FeatureVersionNegotiator
    {
        /// <summary>
        /// Determines if the specified protocol version is supported.
        /// </summary>
        /// <param name="protocolVersion">The current protocol version.</param>
        /// <param name="minimumVersion">The minimum protocol version required.</param>
        /// <returns>True if the protocol version is supported; otherwise, false.</returns>
        private static bool SupportsProtocolVersion(TProtocolVersion protocolVersion, TProtocolVersion minimumVersion)
        {
            return protocolVersion >= minimumVersion;
        }

        #region Protocol Version V1 Features
        /*
         * V1 introduced the following base features that are always available:
         * - Arrow format support for data exchange
         * - Arrow-based result sets
         * - Result format specification in metadata requests
         * - Information about more rows being available
         * - Session information retrieval during opening
         * - Direct result retrieval
         */

        #endregion

        #region Protocol Version V3 Features

        /**
        * V3 introduced the following features:
        * - Cloud storage-based result sets. driver will use the response from server to determine if cloudfetch can be used
        * - LZ4 compression in results, driver should use response from server to determine if this is supported
        */

        #endregion

        #region Protocol Version V4 Features

        /**
        * V4 introduced the following features:
        * - Initial namespace support, driver will fallback to setting default namespace explicitly if not supported
        * - resultRowLimit (not implemented)
        * - resultSetMetadata shortcut (not implemented)
        * - Multiple catalogs support, driver should use response from server to determine if this is supported
        * - PKFK support: do an explicit check for this feature to prevent redundant roundtrips
        */

        public static bool SupportsPKFK(TProtocolVersion protocolVersion) =>
            SupportsProtocolVersion(protocolVersion, TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4);

        #endregion

        #region Protocol Version V5 Features

        /**
        * V5 introduced the following features:
        * - ArrowNativeTypes (used to prompt server to send arrow-native types)
        * - ArrowMetadata (field is not used)
        */


        #endregion

        #region Protocol Version V6 Features

        #endregion

        #region Protocol Version V7 Features

        /**
        * V7 introduced the following features:
        * - isStagingOperation
        * - resultPersistenceMode: server will determine if this can be enabled
        * - DESC TABLE EXTENDED support

        */

        /// <summary>
        /// Gets whether DESC TABLE EXTENDED queries are supported.
        /// </summary>
        /// <param name="protocolVersion">The current protocol version.</param>
        /// <returns>True if DESC TABLE EXTENDED is supported; otherwise, false.</returns>
        public static bool SupportsDESCTableExtended(TProtocolVersion protocolVersion) =>
            SupportsProtocolVersion(protocolVersion, TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7);

        #endregion

        #region Protocol Version V8 Features

       /**
        * V8 introduced the following features:
        * - Parameterized queries support
        */

        #endregion

        #region Protocol Version V9 Features

        /// <summary>
        /// Gets whether full asynchronous execution of metadata operations is supported. (Not implemented)
        /// </summary>

        #endregion

        /// <summary>
        /// Gets whether the protocol version is a Databricks protocol version.
        /// </summary>
        public static bool IsDatabricksProtocolVersion(TProtocolVersion protocolVersion) =>
            protocolVersion >= TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1;
    }
}
