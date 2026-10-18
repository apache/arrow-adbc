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

namespace Apache.Arrow.Adbc.C
{
    /// <summary>
    /// Tracks the databases an exported driver has open and runs a collection once
    /// the last one has been released and none has been opened for <see cref="Delay"/>.
    /// </summary>
    internal sealed class IdleCollector
    {
        private readonly Action _collect;
        private TimeSpan? _delay;
        private int _open;
        private int _generation;
        private int _collectedGeneration = -1;

        public IdleCollector(Action collect, TimeSpan? delay)
        {
            _collect = collect;
            Delay = delay;
        }

        /// <summary>
        /// How long to wait for the driver to stay idle before collecting, or <c>null</c>
        /// to never collect.
        /// </summary>
        public TimeSpan? Delay
        {
            get => _delay;
            set
            {
                if (value < TimeSpan.Zero)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "The delay cannot be negative.");
                }
                _delay = value;
            }
        }

        public void DatabaseOpened()
        {
            Interlocked.Increment(ref _open);
            Interlocked.Increment(ref _generation);
        }

        public void DatabaseReleased()
        {
            if (Interlocked.Decrement(ref _open) == 0)
            {
                Schedule();
            }
        }

        public void DriverReleased()
        {
            if (Volatile.Read(ref _open) == 0)
            {
                Schedule();
            }
        }

        private void Schedule()
        {
            TimeSpan? delay = _delay;
            if (delay == null)
            {
                return;
            }

            int generation = Volatile.Read(ref _generation);
            Task.Delay(delay.Value).ContinueWith(_ => CollectIfIdle(generation), TaskScheduler.Default);
        }

        private void CollectIfIdle(int generation)
        {
            // Skip if a database was opened since this was scheduled; its release
            // schedules a collection of its own.
            if (Volatile.Read(ref _open) != 0 || Volatile.Read(ref _generation) != generation)
            {
                return;
            }

            // Releasing the last database and then the driver schedules twice for the
            // same idle period, but one collection is enough.
            if (Interlocked.Exchange(ref _collectedGeneration, generation) == generation)
            {
                return;
            }

            _collect();
        }
    }
}
