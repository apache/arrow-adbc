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

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Used to validate the expected .NET type, array type and value for tests
    /// </summary>
    public class ColumnNetTypeArrowTypeValue
    {
        public ColumnNetTypeArrowTypeValue(string name, Type expectedNetType, Type expectedArrowArrayType, object? expectedValue)
        {
            this.Name = name;
            this.ExpectedNetType = expectedNetType;
            this.ExpectedArrowArrayType = expectedArrowArrayType;
            this.ExpectedValue = expectedValue;
        }

        public string Name { get; set; }

        public Type ExpectedNetType { get; set; }

        public Type ExpectedArrowArrayType { get; set; }

        public object? ExpectedValue { get; set; }
    }
}
