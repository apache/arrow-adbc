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
using Apache.Arrow.Adbc.Core;

namespace Apache.Arrow.Adbc.FlightSql
{
    /// <summary>
    /// A Flight SQL implementation of <see cref="AdbcDatabase"/>.
    /// </summary>
    public class FlightSqlDatabase : AdbcDatabase
    {
        readonly Dictionary<string, string> metadata;

        public FlightSqlDatabase(Dictionary<string, string> metadata)
        {
            this.metadata = metadata;
        }

        public override AdbcConnection Connect(Dictionary<string, string> options)
        {
            if(options == null) throw new ArgumentNullException("options");

            if (!options.ContainsKey(FlightSqlParameters.ServerAddress))
                throw new ArgumentException($"Options must include the {FlightSqlParameters.ServerAddress} parameter");

            FlightSqlConnection connection = new FlightSqlConnection(this.metadata);
            connection.Open(options[FlightSqlParameters.ServerAddress]);
            return connection;
        }
    }
}
