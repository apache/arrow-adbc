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
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    public class IArrowArrayExtensionsTests
    {
        [Fact]
        public void ValidateTimestamp()
        {
            DateTimeOffset theFuture = new DateTimeOffset(new DateTime(9999, 12, 31, 0, 0, 0), TimeSpan.Zero);
            TimestampArray.Builder theFutureBuilder = new TimestampArray.Builder(TimestampType.Default);
            theFutureBuilder.Append(theFuture);
            TimestampArray tsFutureArray = theFutureBuilder.Build();

            Assert.Equal(theFuture, tsFutureArray.GetTimestamp(0));

            Assert.Equal(theFuture, tsFutureArray.ValueAt(0));
            Assert.Equal(theFuture, tsFutureArray.Data.DataType.GetValueConverter().Invoke(tsFutureArray, 0));
        }

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

#if NET6_0_OR_GREATER
            TimeOnly timeOnlySeconds = new TimeOnly(t.Ticks);
            Assert.Equal(timeOnlySeconds, t32seconds.ValueAt(0));
            Assert.Equal(timeOnlySeconds, t32seconds.Data.DataType.GetValueConverter().Invoke(t32seconds, 0));
#else
            Assert.Equal(t, t32seconds.ValueAt(0));
            Assert.Equal(t, t32seconds.Data.DataType.GetValueConverter().Invoke(t32seconds, 0));
#endif
            int totalMs = Convert.ToInt32(t.TotalMilliseconds);
            Time32Array.Builder msbuilder = new Time32Array.Builder(Types.TimeUnit.Millisecond);
            msbuilder.Append(totalMs);
            Time32Array t32ms = msbuilder.Build();

            Assert.Equal(totalMs, t32ms.GetValue(0));
            Assert.Equal(totalMs, t32ms.GetMilliSeconds(0));

#if NET6_0_OR_GREATER
            TimeOnly timeOnlyMs = new TimeOnly(t.Ticks);
            Assert.Equal(timeOnlyMs, t32ms.ValueAt(0));
            Assert.Equal(timeOnlyMs, t32ms.Data.DataType.GetValueConverter().Invoke(t32ms, 0));
#else
            Assert.Equal(t, t32ms.ValueAt(0));
            Assert.Equal(t, t32ms.Data.DataType.GetValueConverter().Invoke(t32ms, 0));
#endif
        }

        [Fact]
        public void ValidateTime64()
        {
            TimeSpan t = TimeSpan.FromMinutes(5);
            long microseconds = Convert.ToInt64(t.TotalMinutes * 60 * 1_000_000);
            Time64Array.Builder secondBuilder = new Time64Array.Builder(Types.TimeUnit.Microsecond);
            secondBuilder.Append(microseconds);
            Time64Array t64microseconds = secondBuilder.Build();

            Assert.Equal(microseconds, t64microseconds.GetValue(0));
            Assert.Equal(microseconds, t64microseconds.GetMicroSeconds(0));

#if NET6_0_OR_GREATER
            TimeOnly timeOnlyMicroseconds = new TimeOnly(t.Ticks);
            Assert.Equal(timeOnlyMicroseconds, t64microseconds.ValueAt(0));
            Assert.Equal(timeOnlyMicroseconds, t64microseconds.Data.DataType.GetValueConverter().Invoke(t64microseconds, 0));
#else
            Assert.Equal(t, t64microseconds.ValueAt(0));
            Assert.Equal(t, t64microseconds.Data.DataType.GetValueConverter().Invoke(t64microseconds, 0));
#endif
            long nanoseconds = Convert.ToInt64(t.TotalMinutes * 60 * 1_000_000_000);
            Time64Array.Builder msbuilder = new Time64Array.Builder(Types.TimeUnit.Nanosecond);
            msbuilder.Append(nanoseconds);
            Time64Array t64ns = msbuilder.Build();

            Assert.Equal(nanoseconds, t64ns.GetValue(0));
            Assert.Equal(nanoseconds, t64ns.GetNanoSeconds(0));

#if NET6_0_OR_GREATER
            TimeOnly timeOnlyNanoseconds = new TimeOnly(t.Ticks);
            Assert.Equal(timeOnlyNanoseconds, t64ns.ValueAt(0));
            Assert.Equal(timeOnlyNanoseconds, t64ns.Data.DataType.GetValueConverter().Invoke(t64ns, 0));
#else
            Assert.Equal(t, t64ns.ValueAt(0));
            Assert.Equal(t, t64ns.Data.DataType.GetValueConverter().Invoke(t64ns, 0));
#endif
        }
    }
}
