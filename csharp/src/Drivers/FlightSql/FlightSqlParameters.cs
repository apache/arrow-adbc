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

namespace Apache.Arrow.Adbc.Drivers.FlightSql
{
    /// <summary>
    /// Parameters used for connecting to Flight SQL data sources.
    /// </summary>
    public class FlightSqlParameters
    {
        public const string ServerAddress = "FLIGHT_SQL_SERVER_ADDRESS";
        public const string RoutingTag = "routing_tag";
        public const string RoutingQueue = "routing_queue";
        public const string Authorization = "authorization";
    }
}
