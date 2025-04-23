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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Apache.Arrow.Adbc.Extensions;

namespace Apache.Arrow.Adbc.Tests
{
    public class IArrowArrayExtensionsTests
    {

        [Fact]
        public void ValidateTime32()
        {
            TimeSpan t = TimeSpan.FromMinutes(5);
            int seconds = Convert.ToInt32(t.TotalMinutes * 60);
            Time32Array.Builder secondBuilder = new Time32Array.Builder(Types.TimeUnit.Second);
            secondBuilder.Append(seconds);
            Time32Array t32seconds = secondBuilder.Build();

            Assert.Equal(seconds, t32seconds.GetValue(0));
            Assert.Equal(seconds, t32seconds.GetSeconds(0));
            Assert.Equal(t, t32seconds.ValueAt(0));
            Assert.Equal(t, t32seconds.Data.DataType.GetValueConverter().Invoke(t32seconds, 0));

            int totalMs = Convert.ToInt32(t.TotalMilliseconds);
            Time32Array.Builder msbuilder = new Time32Array.Builder(Types.TimeUnit.Millisecond);
            msbuilder.Append(totalMs);
            Time32Array t32ms = msbuilder.Build();

            Assert.Equal(totalMs, t32ms.GetValue(0));
            Assert.Equal(totalMs, t32ms.GetMilliSeconds(0));
            Assert.Equal(t, t32ms.ValueAt(0));
            Assert.Equal(t, t32ms.Data.DataType.GetValueConverter().Invoke(t32ms, 0));
        }

        [Fact]
        public void ValidateTime64() { }

    }
}
