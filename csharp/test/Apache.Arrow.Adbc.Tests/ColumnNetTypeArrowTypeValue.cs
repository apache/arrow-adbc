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
        private Func<object?, bool>? _isValid;

        /// <summary>
        /// Instantiates <see cref="ColumnNetTypeArrowTypeValue"/>.
        /// </summary>
        /// <param name="name">The column name</param>
        /// <param name="expectedNetType">The expected .NET type</param>
        /// <param name="expectedArrowArrayType">The expected Arrow type</param>
        /// <param name="expectedValue">The expected value</param>
        public ColumnNetTypeArrowTypeValue(string name, Type expectedNetType, Type expectedArrowArrayType, object? expectedValue)
        {
            this.Name = name;
            this.ExpectedNetType = expectedNetType;
            this.ExpectedArrowArrayType = expectedArrowArrayType;
            this.ExpectedValue = expectedValue;
        }

        /// <summary>
        /// Instantiates <see cref="ColumnNetTypeArrowTypeValue"/>.
        /// </summary>
        /// <param name="name">The column name</param>
        /// <param name="expectedNetType">The expected .NET type</param>
        /// <param name="expectedArrowArrayType">The expected Arrow type</param>
        /// <param name="expectedValue">The expected value</param>
        /// <param name="isValid">A function that can be run to compare values</param>
        /// <exception cref="ArgumentNullException"></exception>
        public ColumnNetTypeArrowTypeValue(string name, Type expectedNetType, Type expectedArrowArrayType, bool expectedValue, Func<object?, bool> isValid)
        {
            this.Name = name;
            this.ExpectedNetType = expectedNetType;
            this.ExpectedArrowArrayType = expectedArrowArrayType;
            this.ExpectedValue = expectedValue;

            _isValid = isValid ?? throw new ArgumentNullException(nameof(isValid));
        }

        public string Name { get; set; }

        public Type ExpectedNetType { get; set; }

        public Type ExpectedArrowArrayType { get; set; }

        public object? ExpectedValue { get; set; }

        public bool IsCalculatedResult => _isValid != null;

        public bool IsValid(object? value)
        {
            return _isValid == null ? false : Convert.ToBoolean(this.ExpectedValue) == _isValid(value);
        }
    }
}
