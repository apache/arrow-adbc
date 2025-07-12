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
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit
{
    public class DatabricksSchemaParserTests
    {
        [Fact]
        public void GetArrowType_DecimalType_WithUseArrowNativeTypesTrue_ReturnsDecimal128Type()
        {
            // Arrange
            var parser = new DatabricksSchemaParser(useArrowNativeTypes: true);
            var thriftType = CreateDecimalTypeEntry(precision: 10, scale: 2);
            var dataTypeConversion = DataTypeConversion.None;

            // Act
            var result = parser.GetArrowType(thriftType, dataTypeConversion);

            // Assert
            Assert.IsType<Decimal128Type>(result);
            var decimalType = (Decimal128Type)result;
            Assert.Equal(10, decimalType.Precision);
            Assert.Equal(2, decimalType.Scale);
        }

        [Fact]
        public void GetArrowType_DecimalType_WithUseArrowNativeTypesFalse_ReturnsStringType()
        {
            // Arrange
            var parser = new DatabricksSchemaParser(useArrowNativeTypes: false);
            var thriftType = CreateDecimalTypeEntry(precision: 10, scale: 2);
            var dataTypeConversion = DataTypeConversion.None;

            // Act
            var result = parser.GetArrowType(thriftType, dataTypeConversion);

            // Assert
            Assert.IsType<StringType>(result);
        }

        private static TPrimitiveTypeEntry CreateDecimalTypeEntry(int precision, int scale)
        {
            return new TPrimitiveTypeEntry
            {
                Type = TTypeId.DECIMAL_TYPE,
                TypeQualifiers = new TTypeQualifiers
                {
                    Qualifiers = new Dictionary<string, TTypeQualifierValue>
                    {
                        ["precision"] = new TTypeQualifierValue { I32Value = precision },
                        ["scale"] = new TTypeQualifierValue { I32Value = scale }
                    }
                }
            };
        }
} 