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

namespace Apache.Arrow.Adbc.Drivers.FlightSql
{
    /// <summary>
    /// A Flight SQL implementation of <see cref="AdbcDatabase"/>.
    /// </summary>
    public class FlightSqlDatabase : AdbcDatabase
    {
        private readonly IReadOnlyDictionary<string, string>? _metadata;

        public FlightSqlDatabase(IReadOnlyDictionary<string, string>? metadata)
        {
            _metadata = metadata;
        }

        public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
        {
            if (options == null) throw new ArgumentNullException("options");

            string flightSqlServerAddress = string.Empty;

            if (options.TryGetValue(FlightSqlParameters.ServerAddress, out flightSqlServerAddress!))
            {
                FlightSqlConnection connection = new FlightSqlConnection(_metadata);
                connection.Open(flightSqlServerAddress);
                return connection;
            }
            else
            {
                throw new ArgumentException($"Options must include the {FlightSqlParameters.ServerAddress} parameter");
            }
        }
    }
}
