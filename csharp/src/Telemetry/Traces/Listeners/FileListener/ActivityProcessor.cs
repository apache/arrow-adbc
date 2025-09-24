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
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Telemetry.Traces.Listeners.FileListener
{
    internal sealed class ActivityProcessor : IDisposable
    {
        private static readonly byte[] s_newLine = Encoding.UTF8.GetBytes(Environment.NewLine);
        private Task? _processingTask;
        private readonly Channel<Activity> _channel;
        private readonly Func<Stream, CancellationToken, Task> _streamWriterFunc;
        private CancellationTokenSource? _cancellationTokenSource;

        public ActivityProcessor(Func<Stream, CancellationToken, Task> streamWriterFunc)
        {
            _channel = Channel.CreateUnbounded<Activity>();
            _streamWriterFunc = streamWriterFunc;
        }

        public bool TryWrite(Activity activity) => _channel.Writer.TryWrite(activity);

        public async Task<bool> TryStartAsync()
        {
            if (_processingTask != null)
            {
                if (!_processingTask.IsCompleted)
                {
                    // Already running
                    return false;
                }
                await StopAsync().ConfigureAwait(false);
            }
            _cancellationTokenSource = new CancellationTokenSource();
            _processingTask = Task.Run(() => ProcessActivitiesAsync(_cancellationTokenSource.Token));
            return true;
        }

        public async Task StopAsync(int timeout = 5000)
        {
            // Try to gracefully stop to allow processing of all queued items.
            _channel.Writer.TryComplete();
            if (_processingTask != null)
            {
                if (await Task.WhenAny(_processingTask, Task.Delay(timeout)).ConfigureAwait(false) != _processingTask)
                {
                    // Timeout - cancel
                    _cancellationTokenSource?.Cancel();
                    // Assume it will NOT throw any exceptions
                    await _processingTask.ConfigureAwait(false);
                }
                _processingTask.Dispose();
            }
            _processingTask = null;
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;
        }

        private async Task ProcessActivitiesAsync(CancellationToken cancellationToken)
        {
            try
            {
                using MemoryStream stream = new();
                await foreach (Activity activity in _channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    stream.SetLength(0);
                    SerializableActivity serializableActivity = new(activity);
                    await JsonSerializer.SerializeAsync(
                        stream,
                        serializableActivity, cancellationToken: cancellationToken).ConfigureAwait(false);
                    stream.Write(s_newLine, 0, s_newLine.Length);
                    stream.Position = 0;

                    await _streamWriterFunc(stream, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException ex)
            {
                // Expected when cancellationToken is cancelled.
                Trace.TraceError(ex.ToString());
            }
            catch (Exception ex)
            {
                // Since this will be called on an independent thread, we need to avoid uncaught exceptions.
                Trace.TraceError(ex.ToString());
            }
        }

        public void Dispose()
        {
            StopAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }
    }
}
