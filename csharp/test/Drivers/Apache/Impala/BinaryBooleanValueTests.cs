﻿/*
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
using System.Text;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Impala
{
    public class BinaryBooleanValueTests : Common.BinaryBooleanValueTests<ApacheTestConfiguration, ImpalaTestEnvironment>
    {
        public BinaryBooleanValueTests(ITestOutputHelper output)
            : base(output, new ImpalaTestEnvironment.Factory())
        {
        }

        protected override string? GetFormattedBinaryValue(byte[]? value)
        {
            return value != null ? $"CAST ('{Encoding.UTF8.GetString(value)}' as BINARY)" : null;
        }
    }
}