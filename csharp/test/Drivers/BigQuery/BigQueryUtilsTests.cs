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
using Apache.Arrow.Adbc.Drivers.BigQuery;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.BigQuery
{
    public class BigQueryUtilsTests
    {
        [Fact]
        public void TestContainsExceptionWithAggregateInMiddle()
        {
            Exception innerMost = new InvalidOperationException("Innermost exception");
            Exception middle = new AggregateException("Middle exception", innerMost);
            Exception outer = new Exception("Outer exception", middle);
            bool found = BigQueryUtils.ContainsException(outer, out InvalidOperationException? containedException);
            Assert.True(found);
            Assert.NotNull(containedException);
            Assert.Equal(innerMost, containedException);
        }

        [Fact]
        public void TestContainsExceptionWithAggregateOnTop()
        {
            Exception innerMost = new InvalidOperationException("Innermost exception");
            Exception middle = new Exception("Middle exception", innerMost);
            Exception outer = new AggregateException("Outer exception", middle);
            bool found = BigQueryUtils.ContainsException(outer, out InvalidOperationException? containedException);
            Assert.True(found);
            Assert.NotNull(containedException);
            Assert.Equal(innerMost, containedException);
        }

        [Fact]
        public void TestContainsExceptionMultipleAggregate()
        {
            Exception innerMost = new InvalidOperationException("Innermost exception");
            Exception middle1 = new AggregateException("Middle exception 1", innerMost);
            Exception middle2 = new AggregateException("Middle exception 2", middle1);
            Exception outer = new Exception("Outer exception", middle2);
            bool found = BigQueryUtils.ContainsException(outer, out InvalidOperationException? containedException);
            Assert.True(found);
            Assert.NotNull(containedException);
            Assert.Equal(innerMost, containedException);
        }

        [Fact]
        public void TestContainsAggregateException()
        {
            Exception innerMost = new InvalidOperationException("Innermost exception");
            Exception middle = new AggregateException("Middle exception 1", innerMost);
            Exception outer = new Exception("Outer exception", middle);
            bool found = BigQueryUtils.ContainsException(outer, out AggregateException? containedException);
            Assert.True(found);
            Assert.NotNull(containedException);
            Assert.Equal(middle, containedException);
        }
        [Fact]
        public void TestContainsMultipleInAggregate()
        {
            Exception innerMost1 = new InvalidOperationException("Innermost exception 1");
            Exception innerMost2 = new NotImplementedException("Innermost exception 2");
            Exception middle = new AggregateException("Middle exception", [innerMost1, innerMost2]);
            Exception outer = new Exception("Outer exception", middle);
            bool found = BigQueryUtils.ContainsException(outer, out NotImplementedException? containedException);
            Assert.True(found);
            Assert.NotNull(containedException);
            Assert.Equal(innerMost2, containedException);
        }
    }
}
