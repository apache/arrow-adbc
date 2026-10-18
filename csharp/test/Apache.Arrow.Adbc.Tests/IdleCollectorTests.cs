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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.C;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    public class IdleCollectorTests
    {
        private static readonly TimeSpan ShortDelay = TimeSpan.FromMilliseconds(20);

        // Long enough for a collection scheduled with ShortDelay to have run.
        private static readonly TimeSpan Settle = TimeSpan.FromMilliseconds(500);

        private int _collections;

        private IdleCollector Create(TimeSpan? delay) =>
            new IdleCollector(() => Interlocked.Increment(ref _collections), delay);

        private async Task WaitForCollections(int expected)
        {
            DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (Volatile.Read(ref _collections) < expected && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }
            await Task.Delay(Settle);
            Assert.Equal(expected, Volatile.Read(ref _collections));
        }

        [Fact]
        public async Task CollectsAfterLastDatabaseIsReleased()
        {
            IdleCollector collector = Create(ShortDelay);
            collector.DatabaseOpened();
            collector.DatabaseOpened();
            collector.DatabaseReleased();
            await Task.Delay(Settle);
            Assert.Equal(0, Volatile.Read(ref _collections));

            collector.DatabaseReleased();
            await WaitForCollections(1);
        }

        [Fact]
        public async Task CollectsOnceWhenDriverIsReleasedAfterDatabases()
        {
            IdleCollector collector = Create(ShortDelay);
            collector.DatabaseOpened();
            collector.DatabaseReleased();
            collector.DriverReleased();
            await WaitForCollections(1);
        }

        [Fact]
        public async Task CollectsWhenDriverIsReleasedWithoutDatabases()
        {
            IdleCollector collector = Create(ShortDelay);
            collector.DriverReleased();
            await WaitForCollections(1);
        }

        [Fact]
        public async Task SkipsCollectionWhenDatabaseIsReopenedDuringDelay()
        {
            IdleCollector collector = Create(TimeSpan.FromMilliseconds(200));
            collector.DatabaseOpened();
            collector.DatabaseReleased();
            collector.DatabaseOpened();
            await Task.Delay(Settle);
            Assert.Equal(0, Volatile.Read(ref _collections));

            collector.DatabaseReleased();
            await WaitForCollections(1);
        }

        [Fact]
        public async Task CollectsAgainAfterLaterIdlePeriod()
        {
            IdleCollector collector = Create(ShortDelay);
            collector.DatabaseOpened();
            collector.DatabaseReleased();
            await WaitForCollections(1);

            collector.DatabaseOpened();
            collector.DatabaseReleased();
            await WaitForCollections(2);
        }

        [Fact]
        public async Task NullDelayDisablesCollection()
        {
            IdleCollector collector = Create(null);
            collector.DatabaseOpened();
            collector.DatabaseReleased();
            collector.DriverReleased();
            await Task.Delay(Settle);
            Assert.Equal(0, Volatile.Read(ref _collections));
        }

        [Fact]
        public void NegativeDelayIsRejected()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => Create(TimeSpan.FromMilliseconds(-1)));
            IdleCollector collector = Create(null);
            Assert.Throws<ArgumentOutOfRangeException>(() => collector.Delay = TimeSpan.FromMilliseconds(-1));
        }

        [Fact]
        public void ExporterDoesNotCollectByDefaultWhenJitCompiled()
        {
            Assert.Null(CAdbcDriverExporter.IdleCollectionDelay);
        }
    }
}
