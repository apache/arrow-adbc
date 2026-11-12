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
    /// the last one has been released and nothing else has happened for <see cref="Delay"/>.
    /// </summary>
    internal sealed class IdleCollector
    {
        /// <summary>The longest delay that <see cref="Task.Delay(TimeSpan)"/> accepts on every runtime.</summary>
        internal static readonly TimeSpan MaxDelay = TimeSpan.FromMilliseconds(int.MaxValue);

        private const long Disabled = -1;

        private readonly Action _collect;

        // The delay in ticks, or Disabled. Stored as a long so that it's read atomically.
        private long _delayTicks;
        private int _open;
        private int _generation;

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
            get
            {
                long ticks = Interlocked.Read(ref _delayTicks);
                return ticks == Disabled ? null : TimeSpan.FromTicks(ticks);
            }
            set
            {
                if (value < TimeSpan.Zero || value > MaxDelay)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), $"The delay must be between zero and {MaxDelay}.");
                }
                Interlocked.Exchange(ref _delayTicks, value?.Ticks ?? Disabled);
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
            // Releasing the driver makes more objects unreachable, so it starts a new idle
            // period even if the last database's collection has already run.
            Interlocked.Increment(ref _generation);
            if (Volatile.Read(ref _open) == 0)
            {
                Schedule();
            }
        }

        private void Schedule()
        {
            TimeSpan? delay = Delay;
            if (delay == null)
            {
                return;
            }

            int generation = Volatile.Read(ref _generation);
            Task.Delay(delay.Value).ContinueWith(_ => CollectIfIdle(generation), TaskScheduler.Default);
        }

        private void CollectIfIdle(int generation)
        {
            // Skip if collection was disabled while this was pending, or if anything happened
            // since it was scheduled; the latest release schedules a collection of its own.
            if (Delay == null || Volatile.Read(ref _open) != 0 || Volatile.Read(ref _generation) != generation)
            {
                return;
            }

            _collect();
        }
    }
}
