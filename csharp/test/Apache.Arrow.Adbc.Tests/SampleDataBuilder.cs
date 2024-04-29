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

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Used to build and verify sample data against a data source without having to create a table.
    /// </summary>
    public class SampleDataBuilder
    {
        public SampleDataBuilder()
        {
            this.Samples = new List<SampleData>();
        }

        public List<SampleData> Samples { get; set; }
    }

    /// <summary>
    /// Sample data for different data sources.
    /// </summary>
    public class SampleData
    {
        public SampleData()
        {
            this.ExpectedValues = new List<ColumnNetTypeArrowTypeValue>();
        }

        /// <summary>
        /// The query to run.
        /// </summary>
        public string Query { get; set; } = string.Empty;

        /// <summary>
        /// The expected values.
        /// </summary>
        public List<ColumnNetTypeArrowTypeValue> ExpectedValues { get; set; }
    }
}
