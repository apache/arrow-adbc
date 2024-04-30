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

using System.Collections.Generic;

namespace Apache.Arrow.Adbc.Tests.Metadata
{
    /// <summary>
    /// Represents the table in the metadata.
    /// </summary>
    public class AdbcTable
    {
        /// <summary>
        /// Table name
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// Table type
        /// </summary>
        public string? Type { get; set; }

        /// <summary>
        /// List of columns associated with the table.
        /// </summary>
        public List<AdbcColumn>? Columns { get; set; }

        /// <summary>
        /// The constrains associated with the table.
        /// </summary>
        public List<AdbcConstraint>? Constraints { get;  set; }
    }
}
