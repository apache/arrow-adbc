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
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// Helpers for the default implementations of the asynchronous methods on
    /// the ADBC base classes. Those defaults run the synchronous method on the
    /// thread pool and translate a <see cref="CancellationToken"/> into a call
    /// to the object's <c>Cancel</c> method.
    /// </summary>
    internal static class AdbcAsync
    {
        private static readonly Action<object?> s_cancel = TryCancel;

        public static Task<T> Run<T>(Func<T> operation, Action? cancel, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled<T>(cancellationToken);
            }

            return Task.Run(() =>
            {
                CancelState state = new CancelState(cancel);
                Exception failure;
                using (Register(state, cancellationToken))
                {
                    try
                    {
                        return operation();
                    }
                    catch (Exception e) when (!(e is OperationCanceledException))
                    {
                        failure = e;
                    }
                }

                // Disposing the registration waits for an in-progress Cancel to finish
                throw Translate(failure, state, cancellationToken);
            }, cancellationToken);
        }

        public static Task Run(Action operation, Action? cancel, CancellationToken cancellationToken)
        {
            return Run<bool>(() => { operation(); return true; }, cancel, cancellationToken);
        }

        /// <summary>
        /// Attaches cancellation to a task produced by a driver override that
        /// doesn't itself accept a <see cref="CancellationToken"/>.
        /// </summary>
        public static async Task<T> WithCancellation<T>(Task<T> task, Action? cancel, CancellationToken cancellationToken)
        {
            CancelState state = new CancelState(cancel);
            Exception failure;
            using (Register(state, cancellationToken))
            {
                try
                {
                    return await task.ConfigureAwait(false);
                }
                catch (Exception e) when (!(e is OperationCanceledException))
                {
                    failure = e;
                }
            }

            throw Translate(failure, state, cancellationToken);
        }

        private static CancellationTokenRegistration Register(CancelState state, CancellationToken cancellationToken)
        {
            if (state.Cancel == null || !cancellationToken.CanBeCanceled)
            {
                return default;
            }
            return cancellationToken.Register(s_cancel, state);
        }

        /// <summary>
        /// Reports a failure as cancellation only if cancellation plausibly caused it: the
        /// token was cancelled and either the driver accepted the call to Cancel or the
        /// failure itself says the operation was cancelled. Otherwise, a driver that can't
        /// cancel would have its own errors hidden behind <see cref="OperationCanceledException"/>.
        /// </summary>
        private static Exception Translate(Exception failure, CancelState state, CancellationToken cancellationToken)
        {
            bool cancelled = cancellationToken.IsCancellationRequested &&
                (state.Delivered || (failure is AdbcException adbc && adbc.Status == AdbcStatusCode.Cancelled));
            if (cancelled)
            {
                return new OperationCanceledException(failure.Message, failure, cancellationToken);
            }

            ExceptionDispatchInfo.Capture(failure).Throw();
            return failure; // not reached
        }

        private static void TryCancel(object? state)
        {
            CancelState cancelState = (CancelState)state!;
            try
            {
                cancelState.Cancel!();
                cancelState.Delivered = true;
            }
            catch (Exception)
            {
                // Cancellation is best-effort. A driver that doesn't support
                // cancellation simply runs the operation to completion.
            }
        }

        private sealed class CancelState
        {
            public CancelState(Action? cancel) { Cancel = cancel; }

            public Action? Cancel { get; }

            // Set when Cancel returned without throwing
            public volatile bool Delivered;
        }
    }
}
